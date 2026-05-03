# paper_id_search_module.py

import os, time, json, math, gzip, pickle, requests
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, udf, lit, coalesce
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType,
    ArrayType, DoubleType
)

# for UI
from IPython.display import display, HTML, IFrame
import ipywidgets as widgets
from pyvis.network import Network


# ─── 1. SPARK INITIALIZATION ──────────────────────────────────────────────────

def initialize_spark(
    app_name: str = "ResearchGraphByID",
    driver_memory: str = "14g",
    shuffle_partitions: int = 32
):
    """Initialize SparkSession with GraphFrames support."""
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.driver.memory", driver_memory) \
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions)) \
        .config("spark.jars.packages", "graphframes:graphframes:0.8.4-spark3.5-s_2.13") \
        .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    return spark, sc


# ─── 2. SCHEMA DEFINITION ─────────────────────────────────────────────────────

def define_schema():
    """Schema for OpenAlex work JSON (normalized)."""
    return StructType([
        StructField("paperId", StringType(), False),
        StructField("title",   StringType(), True),
        StructField("year",    IntegerType(), True),
        StructField("referenced_works", ArrayType(StringType()), True),
        StructField("cited_by_count", IntegerType(), True)
    ])


# ─── 3. OPENALEX API CONFIG ──────────────────────────────────────────────────

API_BASE = "https://api.openalex.org"
API_KEY = "MDWHtY4UnD5CKLVBenpBt4"


def _openalex_params(extra=None):
    """Build base query params with API key."""
    params = {"api_key": API_KEY}
    if extra:
        params.update(extra)
    return params


# ─── 4. API FETCH ────────────────────────────────────────────────────────────

def fetch_works_by_ids(openalex_ids):
    """
    Fetch metadata for a list of OpenAlex work IDs.
    Uses filter with OR (pipe) syntax for batch lookup.
    Returns list of normalized paper dicts.
    """
    session = requests.Session()
    results = []

    # OpenAlex supports up to 50 IDs per OR filter
    batch_size = 50
    for i in range(0, len(openalex_ids), batch_size):
        batch = openalex_ids[i:i + batch_size]
        if i > 0:
            time.sleep(1)  # throttle

        # Build OR filter: id:W123|W456|...
        id_filter = "|".join(
            f"https://openalex.org/{oid}" if not oid.startswith("http") else oid
            for oid in batch
        )
        params = _openalex_params({
            "filter": f"openalex_id:{id_filter}",
            "per_page": 50,
            "select": "id,title,publication_year,referenced_works,cited_by_count"
        })

        r = session.get(f"{API_BASE}/works", params=params)
        r.raise_for_status()
        data = r.json().get("results", [])
        print(f"Fetched batch {i // batch_size + 1}: {len(data)} works")

        for w in data:
            results.append(_normalize_work(w))

    return results


def _normalize_work(w):
    """Normalize an OpenAlex work object to internal format."""
    return {
        "paperId": w["id"].replace("https://openalex.org/", ""),
        "title": w.get("title") or w.get("display_name", ""),
        "year": w.get("publication_year"),
        "referenced_works": [
            ref.replace("https://openalex.org/", "")
            for ref in (w.get("referenced_works") or [])
        ],
        "cited_by_count": w.get("cited_by_count", 0)
    }


# ─── 5. GRAPH BUILDERS ───────────────────────────────────────────────────────

def build_graph_from_ids(spark, sc, seed_ids, min_similarity=0.0):
    """
    Given a list of OpenAlex work IDs, fetch their metadata + 1-hop references,
    build a directed citation graph.
    """
    schema = define_schema()

    # 1) normalize IDs (strip URL prefix if present)
    seed_ids = [
        sid.replace("https://openalex.org/", "") for sid in seed_ids
    ]

    # 2) fetch seed metadata
    seed_papers = fetch_works_by_ids(seed_ids)
    if not seed_papers:
        print("No papers found for the given IDs.")
        return None

    print(f"Seed papers fetched: {len(seed_papers)}")

    # 3) extract 1-hop referenced IDs
    cited_ids = set()
    for p in seed_papers:
        for ref in (p.get("referenced_works") or []):
            if ref not in seed_ids:
                cited_ids.add(ref)

    # 4) fetch cited metadata (limit to first 200 for performance)
    cited_ids = list(cited_ids)[:200]
    cited_papers = []
    if cited_ids:
        cited_papers = fetch_works_by_ids(cited_ids)
        print(f"Referenced papers fetched: {len(cited_papers)}")

    # 5) union into full paper set
    all_papers = seed_papers + cited_papers
    seen = set()
    unique_papers = []
    for p in all_papers:
        if p["paperId"] not in seen:
            seen.add(p["paperId"])
            unique_papers.append(p)

    # 6) build vertices DF
    rdd = sc.parallelize(unique_papers)
    vertices = (
        spark.read.schema(schema)
             .json(rdd.map(json.dumps))
             .dropDuplicates(["paperId"])
             .withColumnRenamed("paperId", "id")
             .select("id", "title", "year", "referenced_works")
    )

    v_count = vertices.count()
    print(f"Vertices built: {v_count}")

    # 7) build directed citation edges A→B
    refs = (
        vertices
        .select(col("id").alias("src"), explode("referenced_works").alias("dst"))
        .dropDuplicates(["src", "dst"])
    )
    # restrict to IDs in our vertex set
    valid_ids = vertices.select(col("id").alias("dst"))
    edges = (
        refs.join(valid_ids, on="dst", how="inner")
            .withColumn("weight", lit(1.0))
            .select("src", "dst", "weight")
    )

    e_count = edges.count()
    print(f"Citation edges: {e_count}")

    if e_count == 0:
        print("No citation edges found; building fallback connectivity edges.")
        edges = build_fallback_edges(vertices)

    # 8) assemble GraphFrame
    from graphframes import GraphFrame
    if SparkSession.getActiveSession() is None:
        import graphframes.graphframe as _gf_mod
        _orig_init = GraphFrame.__init__
        def _patched_init(self, v, e):
            self._vertices = v
            self._edges = e
            self._spark = v.sparkSession
            self._sc = spark.sparkContext
            self._jvm_gf_api = _gf_mod._java_api(self._sc)
            self.ID = self._jvm_gf_api.ID()
            self.SRC = self._jvm_gf_api.SRC()
            self.DST = self._jvm_gf_api.DST()
        GraphFrame.__init__ = _patched_init

    g = GraphFrame(vertices.select("id", "title", "year"), edges)
    g.vertices.cache()
    g.edges.cache()
    print(f"✅ Graph: {g.vertices.count()} vertices, {g.edges.count()} edges")
    return g


def build_fallback_edges(vertices):
    """Build star-topology edges from first vertex to all others as fallback."""
    ids = [r.id for r in vertices.select("id").collect()]
    if len(ids) <= 1:
        schema = StructType([
            StructField("src", StringType(), False),
            StructField("dst", StringType(), False),
            StructField("weight", DoubleType(), False)
        ])
        return vertices.sparkSession.createDataFrame([], schema)
    data = []
    for other in ids[1:]:
        data += [(ids[0], other, 1.0), (other, ids[0], 1.0)]
    schema = StructType([
        StructField("src", StringType(), False),
        StructField("dst", StringType(), False),
        StructField("weight", DoubleType(), False)
    ])
    return vertices.sparkSession.createDataFrame(data, schema)


# ─── 6. LOUVAIN COMMUNITY DETECTION ──────────────────────────────────────────

def compute_louvain_communities(g, resolution=1.0):
    """
    Run Louvain community detection on the citation network.
    Uses networkx + community (python-louvain) for modularity-based detection.
    Falls back to GraphFrames labelPropagation if networkx is unavailable.

    Returns a Spark DataFrame with columns: id, community, title, year
    """
    try:
        import networkx as nx
        from community import community_louvain

        edges_collected = g.edges.select("src", "dst", "weight").collect()
        vertices_collected = g.vertices.select("id").collect()

        G = nx.Graph()
        for v in vertices_collected:
            G.add_node(v.id)
        for e in edges_collected:
            if G.has_edge(e.src, e.dst):
                G[e.src][e.dst]["weight"] += e.weight
            else:
                G.add_edge(e.src, e.dst, weight=e.weight)

        partition = community_louvain.best_partition(
            G, weight="weight", resolution=resolution, random_state=42
        )

        n_communities = len(set(partition.values()))
        modularity = community_louvain.modularity(partition, G, weight="weight")
        print(f"Louvain detected {n_communities} communities (modularity={modularity:.4f})")

        community_data = [(node, int(comm)) for node, comm in partition.items()]
        schema = StructType([
            StructField("id", StringType(), False),
            StructField("community", IntegerType(), False)
        ])
        spark = g.vertices.sparkSession
        community_df = spark.createDataFrame(community_data, schema)
        result = community_df.join(g.vertices.select("id", "title"), on="id")
        return result

    except ImportError:
        print("networkx/python-louvain not available, falling back to LabelPropagation.")
        lp = g.labelPropagation(maxIter=5)
        result = lp.withColumnRenamed("label", "community")
        n_communities = result.select("community").distinct().count()
        print(f"LabelPropagation detected {n_communities} communities")
        return result


def get_community_summary(community_df):
    """Summarize communities: size and representative papers per community."""
    from pyspark.sql.functions import count, collect_list, struct
    summary = (
        community_df
        .groupBy("community")
        .agg(
            count("id").alias("size"),
            collect_list(struct("id", "title")).alias("papers")
        )
        .orderBy(col("size").desc())
    )
    return summary


# ─── 7. PAGERANK ───────────────────────────────────────────────────────────────

def compute_pagerank(g, reset_prob=0.15, max_iter=10):
    """DataFrame‐based weighted PageRank on GraphFrame."""
    ranks = g.vertices.select("id").withColumn("rank", lit(1.0))
    for _ in range(max_iter):
        contribs = (g.edges
            .join(ranks.withColumnRenamed("id", "src"), on="src")
            .withColumn("contrib", col("rank") * col("weight"))
            .select(col("dst").alias("id"), "contrib")
        )
        summed = contribs.groupBy("id").sum("contrib").withColumnRenamed("sum(contrib)", "sumContrib")
        ranks = (g.vertices.select("id")
            .join(summed, on="id", how="left")
            .withColumn("rank", lit(reset_prob) + (1 - reset_prob) * coalesce(col("sumContrib"), lit(0.0)))
            .select("id", "rank")
        )
    return ranks.join(g.vertices.select("id", "title"), on="id")


# ─── 8. VISUALIZATION ──────────────────────────────────────────────────────────

def _rank_color(rank, min_rank, max_rank):
    """Map a PageRank value to a hex color (light blue -> dark blue)."""
    if max_rank == min_rank:
        t = 0.5
    else:
        t = (rank - min_rank) / (max_rank - min_rank)
    r = int(220 - t * 180)
    g = int(235 - t * 135)
    b = int(255 - t * 55)
    return f"#{r:02x}{g:02x}{b:02x}"


_COMMUNITY_COLORS = [
    "#e6194b", "#3cb44b", "#ffe119", "#4363d8", "#f58231",
    "#911eb4", "#42d4f4", "#f032e6", "#bfef45", "#fabed4",
    "#469990", "#dcbeff", "#9a6324", "#fffac8", "#800000",
    "#aaffc3", "#808000", "#ffd8b1", "#000075", "#a9a9a9",
]


def generate_interactive_graph(g, output_file, project_root: Path, height="800px", width="100%", color_by_community=True):
    """PyVis interactive viz of GraphFrame g, colored by Louvain community."""
    pr = compute_pagerank(g).orderBy(col("rank").desc()).limit(50).collect()
    rank_map = {r.id: r.rank for r in pr}
    top_ids = {r.id for r in pr[:10]}
    ranks = list(rank_map.values())
    min_r, max_r = (min(ranks), max(ranks)) if ranks else (0, 1)

    # Compute community assignments for coloring
    community_map = {}
    if color_by_community:
        try:
            comm_df = compute_louvain_communities(g)
            for row in comm_df.select("id", "community").collect():
                community_map[row.id] = row.community
        except Exception:
            pass

    net = Network(height=height, width=width, directed=True, notebook=True, cdn_resources="in_line")
    net.set_options("""{
      "nodes": {
        "font": {"size": 14, "face": "Arial", "color": "#333333"},
        "borderWidth": 1.5,
        "borderWidthSelected": 3,
        "shadow": {"enabled": true, "size": 6, "x": 2, "y": 2}
      },
      "edges": {
        "smooth": {"type": "continuous"},
        "arrows": {"to": {"scaleFactor": 0.4}},
        "color": {"color": "rgba(90, 130, 180, 0.4)"},
        "selectionWidth": 2
      },
      "physics": {
        "forceAtlas2Based": {
          "gravitationalConstant": -80,
          "centralGravity": 0.004,
          "springLength": 200,
          "springConstant": 0.04,
          "damping": 0.5,
          "avoidOverlap": 0.6
        },
        "solver": "forceAtlas2Based",
        "stabilization": {"iterations": 200}
      },
      "interaction": {
        "hover": true,
        "tooltipDelay": 100,
        "zoomView": true,
        "dragView": true
      }
    }""")

    for v in g.vertices.collect():
        rank = rank_map.get(v.id, min_r)
        is_top = v.id in top_ids
        size = 15 + 35 * ((rank - min_r) / (max_r - min_r) if max_r > min_r else 0.5)
        label = (v.title[:40] + "...") if is_top and v.title and len(v.title) > 40 else (v.title if is_top else " ")
        comm = community_map.get(v.id, 0)
        color = _COMMUNITY_COLORS[comm % len(_COMMUNITY_COLORS)] if community_map else _rank_color(rank, min_r, max_r)
        tooltip = f"{v.title or 'Untitled'}\nYear: {v.year}\nCommunity: {comm}\nPageRank: {rank:.4f}"
        net.add_node(v.id,
                     label=label,
                     title=tooltip,
                     size=size,
                     color=color,
                     shape="dot")

    for e in g.edges.collect():
        w = e.weight or 0.0
        thickness = 0.3 + w * 3.0
        net.add_edge(e.src, e.dst,
                     value=thickness,
                     title=f"Weight: {w:.3f}",
                     color="rgba(90, 130, 180, 0.4)")

    out_dir = project_root / "output"
    out_dir.mkdir(parents=True, exist_ok=True)
    old = os.getcwd()
    os.chdir(out_dir)
    net.write_html(output_file)
    os.chdir(old)

    full = out_dir / output_file
    try:
        rel_path = full.resolve().relative_to(Path.cwd().resolve())
    except ValueError:
        rel_path = full.resolve()
    display(IFrame(src=str(rel_path), width="100%", height=height))
    return str(full)


# ─── 9. PARQUET SAVE ──────────────────────────────────────────────────────────

def save_graph_parquet(g, name, project_root: Path, shuffle_partitions: int = 32):
    """Persist vertices & edges as Parquet under project_root/processed/…"""
    v_out = project_root / "processed" / "vertices"
    e_out = project_root / "processed" / "edges_weighted"
    g.vertices.select("id", "title", "year").write.mode("overwrite").partitionBy("year").parquet(str(v_out))
    g.edges.repartition(shuffle_partitions).write.mode("overwrite").parquet(str(e_out))
    return str(v_out), str(e_out)


# ─── 10. WIDGETS ──────────────────────────────────────────────────────────────

def build_id_graph_widget(spark, sc):
    """Single UI to input OpenAlex work IDs, build & visualize the citation graph."""
    txt = widgets.Text(
        description="IDs:",
        placeholder="e.g. W2741809807, W2100837269"
    )
    btn = widgets.Button(description="Build Graph", button_style="primary")
    out = widgets.Output()

    def on_click(b):
        with out:
            out.clear_output()
            # parse IDs
            seed_ids = [i.strip() for i in txt.value.split(",") if i.strip()]
            if not seed_ids:
                print("Please enter at least one OpenAlex work ID.")
                return

            print("Building graph for IDs:", seed_ids)
            g = build_graph_from_ids(spark, sc, seed_ids)

            if g is None:
                print("Graph construction failed.")
                return

            # show PageRank top-10
            pr = compute_pagerank(g).orderBy(col("rank").desc()).limit(10)

            # save to parquet
            v_path, e_path = save_graph_parquet(g, "id_graph", PROJECT_ROOT)
            print(f"Vertices written to {v_path}\nEdges written to {e_path}")

            # render interactive HTML
            html_path = generate_interactive_graph(g, "id_graph.html", PROJECT_ROOT)
            print(f"Interactive graph saved & displayed from:\n  {html_path}")

    btn.on_click(on_click)
    display(widgets.VBox([txt, btn, out]))

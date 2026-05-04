# publication_search_module.py

import os
import time
import json
import math
import requests
import gzip
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, lit, coalesce, explode, countDistinct, sum as spark_sum
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
    app_name: str = "ResearchGraphPublication",
    driver_memory: str = "14g",
    shuffle_partitions: int = 32
):
    """Initialize SparkSession with GraphFrames support."""
    ivy_dir = Path(__file__).resolve().parent.parent / ".spark_ivy"
    ivy_dir.mkdir(parents=True, exist_ok=True)
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.driver.memory", driver_memory) \
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions)) \
        .config("spark.jars.ivy", str(ivy_dir)) \
        .config("spark.jars.packages", "graphframes:graphframes:0.8.4-spark3.5-s_2.13") \
        .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    return spark, sc


# ─── 2. SCHEMA DEFINITION ─────────────────────────────────────────────────────

def define_schema():
    """Spark schema matching OpenAlex Works JSON."""
    return StructType([
        StructField("paperId", StringType(), False),
        StructField("title",   StringType(), True),
        StructField("venue",   StringType(), True),
        StructField("year",    IntegerType(), True),
        StructField("authors", ArrayType(
            StructType([
                StructField("authorId", StringType(), True),
                StructField("name", StringType(), True)
            ])
        ), True),
        StructField("cited_by_count", IntegerType(), True),
        StructField("referenced_works", ArrayType(StringType()), True)
    ])


# ─── 3. OPENALEX API CONFIG ───────────────────────────────────────────────────

API_BASE = "https://api.openalex.org"
API_KEY = "MDWHtY4UnD5CKLVBenpBt4"
NYU_INSTITUTION = "i57206974"
PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _openalex_params(extra=None):
    """Build base query params with API key."""
    params = {"api_key": API_KEY}
    if extra:
        params.update(extra)
    return params


def _normalize_authors(authorships):
    authors = []
    for auth in (authorships or []):
        author = auth.get("author", {})
        authors.append({
            "authorId": (author.get("id") or "").replace("https://openalex.org/", ""),
            "name": author.get("display_name", "")
        })
    return authors


def _save_raw_payload(payload, query_name, project_root: Path = PROJECT_ROOT):
    raw_dir = Path(project_root) / "publication_search" / "raw_json"
    raw_dir.mkdir(parents=True, exist_ok=True)
    safe_name = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in query_name)[:80]
    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    raw_path = raw_dir / f"{safe_name}_{timestamp}.json.gz"
    with gzip.open(raw_path, "wt", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2)
    print(f"Saved raw OpenAlex response to {raw_path}")
    return raw_path


# ─── 4. PUBLICATION‐YEAR SEARCH ────────────────────────────────────────────────

def publication_search(
    publication: str,
    year: int,
    limit: int = 100,
    page: int = 1,
    use_cursor=None,
    save_raw_json: bool = False,
    project_root: Path = PROJECT_ROOT,
):
    """
    Search OpenAlex for NYU-affiliated papers matching a publication/venue name
    and year. Returns normalized paper dicts.
    """
    url = f"{API_BASE}/works"
    session = requests.Session()
    papers = []
    raw_payloads = []
    seen_ids = set()
    per_page = min(limit, 200)
    use_cursor = limit > 100 if use_cursor is None else use_cursor
    cursor = "*"
    current_page = page

    while len(papers) < limit:
        params = _openalex_params({
            "filter": f"authorships.institutions.lineage:{NYU_INSTITUTION},publication_year:{year}",
            "search": publication,
            "per_page": per_page,
            "select": "id,title,publication_year,primary_location,authorships,referenced_works,cited_by_count"
        })
        if use_cursor:
            params["cursor"] = cursor
        else:
            params["page"] = current_page

        r = session.get(url, params=params)
        r.raise_for_status()
        payload = r.json()
        data = payload.get("results", [])
        if save_raw_json:
            raw_payloads.append(payload)
        if not data:
            break

        for w in data:
            source_name = ""
            if w.get("primary_location") and w["primary_location"].get("source"):
                source_name = w["primary_location"]["source"].get("display_name", "")
            normalized = {
                "paperId": w["id"].replace("https://openalex.org/", ""),
                "title": w.get("title") or w.get("display_name", ""),
                "venue": source_name,
                "year": w.get("publication_year"),
                "authors": _normalize_authors(w.get("authorships")),
                "cited_by_count": w.get("cited_by_count", 0),
                "referenced_works": [
                    ref.replace("https://openalex.org/", "")
                    for ref in (w.get("referenced_works") or [])
                ]
            }
            if normalized["paperId"] not in seen_ids:
                seen_ids.add(normalized["paperId"])
                papers.append(normalized)
            if len(papers) >= limit:
                break

        if use_cursor:
            cursor = payload.get("meta", {}).get("next_cursor")
            if not cursor:
                break
        else:
            if len(data) < per_page:
                break
            current_page += 1

    if save_raw_json and raw_payloads:
        _save_raw_payload({
            "publication": publication,
            "year": year,
            "limit": limit,
            "use_cursor": use_cursor,
            "pages": raw_payloads,
        }, query_name=f"publication_{publication}_{year}", project_root=project_root)

    return papers


# ─── 5. GRAPH BUILDER ────────────────────────────────────────────────────────

def build_graph_from_publication(
    spark, sc,
    publication: str, year: int,
    limit: int = 100,
    min_similarity: float = 0.0,
    save_raw_json: bool = False,
):
    schema = define_schema()

    # ─── 1) fetch papers ───────────────────────────────────────────
    papers = publication_search(
        publication,
        year,
        limit=limit,
        save_raw_json=save_raw_json,
        project_root=PROJECT_ROOT,
    )
    print(f"🔍 Papers fetched: {len(papers)}")
    if len(papers) < 2:
        print("Need at least 2 papers to build edges.")
        return None

    # ─── 2) build vertices DF ───────────────────────────────────
    rdd = sc.parallelize(papers)
    vertices = (
        spark.read.schema(schema)
             .json(rdd.map(json.dumps))
             .dropDuplicates(["paperId"])
             .withColumnRenamed("paperId", "id")
             .select("id", "title", "venue", "year", "authors", "cited_by_count", "referenced_works")
    )
    v_count = vertices.count()
    print(f"📊 Vertices built: {v_count}")
    if v_count < 2:
        print("Not enough distinct IDs to form edges.")
        return None

    # ─── 3) build citation edges ─────────────────────────────────
    # Edge A→B if paper A references paper B (and B is in our vertex set)
    refs = (
        vertices
        .select(col("id").alias("src"), explode("referenced_works").alias("dst"))
        .dropDuplicates(["src", "dst"])
    )
    # keep only edges where dst is in our vertex set
    valid_ids = vertices.select(col("id").alias("dst"))
    edges = (
        refs.join(valid_ids, on="dst", how="inner")
            .withColumn("weight", lit(1.0))
            .select("src", "dst", "weight")
    )

    e_count = edges.count()
    print(f"🔗 Citation edges found: {e_count}")

    if e_count == 0:
        # fallback: create edges between all pairs (undirected) with weight=1
        print("No citation edges within set; building co-occurrence edges.")
        v1 = vertices.select(col("id").alias("src"))
        v2 = vertices.select(col("id").alias("dst"))
        edges = (
            v1.crossJoin(v2)
              .filter(col("src") < col("dst"))
              .withColumn("weight", lit(1.0))
              .select("src", "dst", "weight")
        )
        e_count = edges.count()
        if e_count == 0:
            print("No edges could be built.")
            return None

    # ─── 4) make directed ─────────────────────────────────────────
    edges_dir = edges.union(
        edges.select(col("dst").alias("src"),
                     col("src").alias("dst"),
                     col("weight"))
    )

    # ─── 5) assemble GraphFrame ─────────────────────────────────
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
    g = GraphFrame(
        vertices.select("id", "title", "venue", "year", "authors", "cited_by_count"),
        edges_dir
    )
    g.vertices.cache()
    g.edges.cache()

    print(f"✅ Built graph with {g.vertices.count()} nodes "
          f"and {g.edges.count()} edges")
    return g


# ─── 6. LOUVAIN COMMUNITY DETECTION ──────────────────────────────────────────

def compute_louvain_communities(g, resolution=1.0):
    """
    Run Louvain community detection on the citation network.
    Uses networkx + community (python-louvain) for modularity-based detection.
    Falls back to GraphFrames labelPropagation if networkx is unavailable.

    Returns a Spark DataFrame with columns: id, community, title, venue, year
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
        result = community_df.join(g.vertices.select("id", "title", "venue", "year"), on="id")
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

def compute_pagerank(g, reset_prob: float = 0.15, max_iter: int = 10):
    """Run weighted PageRank (DataFrame‐based) on GraphFrame g."""
    ranks = g.vertices.select("id").withColumn("rank", lit(1.0))
    for _ in range(max_iter):
        contribs = (g.edges
            .join(ranks.withColumnRenamed("id", "src"), on="src")
            .withColumn("contrib", col("rank") * col("weight"))
            .select(col("dst").alias("id"), "contrib")
        )
        summed = contribs.groupBy("id") \
                         .sum("contrib") \
                         .withColumnRenamed("sum(contrib)", "sumContrib")
        ranks = (g.vertices.select("id")
            .join(summed, on="id", how="left")
            .withColumn("rank", lit(reset_prob) +
                                  (1 - reset_prob) * coalesce(col("sumContrib"), lit(0.0)))
            .select("id", "rank")
        )
    return ranks.join(g.vertices.select("id", "title", "venue", "year"), on="id")


def compute_author_influence(g, top_n=None):
    """Aggregate paper-level PageRank into author-level influence scores."""
    if "authors" not in g.vertices.columns:
        print("Author information is not available in this graph.")
        return None

    pagerank_df = compute_pagerank(g).select("id", "rank")
    author_scores = (
        g.vertices
        .select("id", explode("authors").alias("author"))
        .join(pagerank_df, on="id", how="inner")
        .select(
            "id",
            col("author.authorId").alias("authorId"),
            col("author.name").alias("authorName"),
            "rank",
        )
        .filter(col("authorId") != "")
        .groupBy("authorId", "authorName")
        .agg(
            spark_sum("rank").alias("influenceScore"),
            countDistinct("id").alias("paperCount"),
        )
        .orderBy(col("influenceScore").desc(), col("paperCount").desc())
    )
    if top_n:
        return author_scores.limit(top_n)
    return author_scores


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


def generate_interactive_graph(
    g,
    output_file: str,
    project_root: Path,
    height: str = "800px",
    width: str  = "100%",
    color_by_community: bool = True,
):
    """Use PyVis to render & save an interactive citation graph, colored by Louvain community."""
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

    net = Network(height=height, width=width,
                  directed=True, notebook=True, cdn_resources="in_line")
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
        tooltip = f"{v.title or 'Untitled'}\nVenue: {v.venue}\nYear: {v.year}\nCommunity: {comm}\nPageRank: {rank:.4f}"
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
    out_dir.mkdir(exist_ok=True)
    old = os.getcwd()
    os.chdir(out_dir)
    net.write_html(output_file)
    os.chdir(old)

    full_path = out_dir / output_file
    try:
        rel_path = full_path.resolve().relative_to(Path.cwd().resolve())
    except ValueError:
        rel_path = full_path.resolve()
    display(IFrame(src=str(rel_path), width="100%", height=height))
    return str(full_path)


# ─── 9. PARQUET SAVE ──────────────────────────────────────────────────────────

def save_graph_parquet(g, name, project_root: Path, shuffle_partitions: int = 32):
    """Persist vertices & edges as Parquet under project_root/processed/."""
    v_out = project_root / "processed" / f"{name}_vertices"
    e_out = project_root / "processed" / f"{name}_edges_weighted"
    g.vertices.write.mode("overwrite").partitionBy("year").parquet(str(v_out))
    g.edges.repartition(shuffle_partitions).write.mode("overwrite").parquet(str(e_out))
    return str(v_out), str(e_out)


# ─── 10. WIDGET ───────────────────────────────────────────────────────────────

def build_publication_graph_widget(spark, sc):
    """ipywidget UI: input publication & year → build & show citation graph."""
    pub = widgets.Text(description="Publication:", placeholder="e.g. Nature")
    yr  = widgets.IntText(description="Year:", value=2020)
    lim = widgets.IntSlider(description="Max Papers:", min=10, max=1000,
                            step=10, value=100)
    raw = widgets.Checkbox(description="Save raw JSON", value=True)
    btn = widgets.Button(description="Build Graph", button_style="primary")
    out = widgets.Output()

    def on_click(b):
        with out:
            out.clear_output()
            print(f"Searching {pub.value} {yr.value}…")
            g = build_graph_from_publication(
                spark, sc,
                publication=pub.value, year=yr.value,
                limit=lim.value,
                save_raw_json=raw.value,
            )
            if g:
                pr = compute_pagerank(g).orderBy(col("rank").desc()).limit(10)
                pr.show(truncate=False)
                authors = compute_author_influence(g, top_n=10)
                if authors is not None:
                    print("\nTop authors by influence:")
                    authors.show(truncate=False)
                v_path, e_path = save_graph_parquet(g, "pub_graph", PROJECT_ROOT)
                print(f"Vertices written to {v_path}\nEdges written to {e_path}")
                html = generate_interactive_graph(
                    g, "pub_graph.html", PROJECT_ROOT
                )
                print(f"Graph saved to {html}")

    btn.on_click(on_click)
    display(widgets.VBox([pub, yr, lim, raw, btn, out]))

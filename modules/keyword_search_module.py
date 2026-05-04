# keyword_search_module.py

import os
import time
import json
import math
import gzip
import pickle
import requests
from datetime import datetime

from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, udf, lit, coalesce, countDistinct, sum as spark_sum
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType,
    ArrayType, DoubleType
)

# for UI functions
from IPython.display import display, HTML, IFrame
import ipywidgets as widgets
from pyvis.network import Network


# ─── 1. SPARK INITIALIZATION ──────────────────────────────────────────────────

def initialize_spark(
    app_name: str = "ResearchGraphKeyword",
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
        .config("spark.jars.packages", "graphframes:graphframes:0.8.4-spark3.5-s_2.12") \
        .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    return spark, sc


# ─── 2. SCHEMA DEFINITION ─────────────────────────────────────────────────────

def define_schema():
    """Define Spark schema matching normalized OpenAlex works JSON."""
    return StructType([
        StructField("paperId", StringType(), False),
        StructField("title",   StringType(), True),
        StructField("year",    IntegerType(), True),
        StructField("authors", ArrayType(
            StructType([
                StructField("authorId", StringType(), True),
                StructField("name",     StringType(), True)
            ])
        ), True),
        StructField("referenced_works", ArrayType(StringType()), True),
        StructField("cited_by_count", IntegerType(), True)
    ])


# ─── 3. OPENALEX API CONFIG ─────────────────────────────────────────────────

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


def _normalize_work(w):
    return {
        "paperId": w["id"].replace("https://openalex.org/", ""),
        "title": w.get("title") or w.get("display_name", ""),
        "year": w.get("publication_year"),
        "authors": _normalize_authors(w.get("authorships")),
        "referenced_works": [
            ref.replace("https://openalex.org/", "")
            for ref in (w.get("referenced_works") or [])
        ],
        "cited_by_count": w.get("cited_by_count", 0)
    }


def _save_raw_payload(payload, query_name, project_root: Path = PROJECT_ROOT):
    raw_dir = Path(project_root) / "keyword_search" / "raw_json"
    raw_dir.mkdir(parents=True, exist_ok=True)
    safe_name = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in query_name)[:80]
    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    raw_path = raw_dir / f"{safe_name}_{timestamp}.json.gz"
    with gzip.open(raw_path, "wt", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2)
    print(f"Saved raw OpenAlex response to {raw_path}")
    return raw_path


# ─── 4. OPENALEX API HELPERS ─────────────────────────────────────────────────

def keyword_search(
    keywords,
    limit=100,
    page=1,
    use_cursor=None,
    save_raw_json=False,
    project_root: Path = PROJECT_ROOT,
):
    """Search for NYU-affiliated papers by keywords via OpenAlex."""
    if isinstance(keywords, list):
        query = " ".join(keywords)
    else:
        query = keywords

    url = f"{API_BASE}/works"
    session = requests.Session()
    try:
        per_page = min(limit, 200)
        use_cursor = limit > 100 if use_cursor is None else use_cursor
        papers = []
        raw_payloads = []
        seen_ids = set()
        cursor = "*"
        current_page = page

        while len(papers) < limit:
            params = _openalex_params({
                "filter": f"authorships.institutions.lineage:{NYU_INSTITUTION}",
                "search": query,
                "per_page": per_page,
                "select": "id,title,publication_year,authorships,referenced_works,cited_by_count"
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

            for work in data:
                normalized = _normalize_work(work)
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
                "query": query,
                "limit": limit,
                "use_cursor": use_cursor,
                "pages": raw_payloads,
            }, query_name=f"keyword_{query}", project_root=project_root)

        print(f"Found {len(papers)} papers for query \"{query}\"")
        return papers
    except Exception as e:
        print(f"keyword_search error: {e}")
        return []


def fetch_works_by_ids(openalex_ids, save_raw_json=False, raw_tag="keyword_ids", project_root: Path = PROJECT_ROOT):
    """
    Fetch metadata for a list of OpenAlex work IDs using filter OR syntax.
    """
    session = requests.Session()
    results = []
    raw_payloads = []
    batch_size = 50

    for i in range(0, len(openalex_ids), batch_size):
        batch = openalex_ids[i:i + batch_size]
        if i > 0:
            time.sleep(1)

        id_filter = "|".join(
            f"https://openalex.org/{oid}" if not oid.startswith("http") else oid
            for oid in batch
        )
        params = _openalex_params({
            "filter": f"openalex_id:{id_filter}",
            "per_page": 50,
            "select": "id,title,publication_year,authorships,referenced_works,cited_by_count"
        })

        try:
            r = session.get(f"{API_BASE}/works", params=params)
            r.raise_for_status()
            payload = r.json()
            data = payload.get("results", [])
            if save_raw_json:
                raw_payloads.append(payload)
            print(f"Fetched batch {i // batch_size + 1}: {len(data)} works")
            for w in data:
                results.append(_normalize_work(w))
        except Exception as e:
            print(f"fetch_works_by_ids error on batch {i // batch_size + 1}: {e}")

    if save_raw_json and raw_payloads:
        _save_raw_payload({
            "openalex_ids": openalex_ids,
            "batches": raw_payloads,
        }, query_name=raw_tag, project_root=project_root)

    return results


# ─── 5. GRAPH BUILDERS ────────────────────────────────────────────────────────

def build_graph_from_keywords(
    spark, sc, keywords,
    limit=100, central_paper=None, min_similarity=0.0, save_raw_json=False
):
    """Main entry: search by keywords → build citation GraphFrame."""
    schema = define_schema()

    # 1) search
    if isinstance(keywords, str):
        keywords = [k.strip() for k in keywords.split(",")]
    papers = keyword_search(keywords, limit=limit, save_raw_json=save_raw_json, project_root=PROJECT_ROOT)
    if not papers:
        print("No papers found.")
        return None

    # 2) to DF
    json_rdd = sc.parallelize(papers)
    df_seed = spark.read.schema(schema).json(json_rdd.map(json.dumps)).dropDuplicates(["paperId"])

    # 3) optionally add central paper
    ids = df_seed.select("paperId").rdd.map(lambda r: r.paperId).collect()
    if central_paper:
        # normalize central_paper ID
        central_paper = central_paper.replace("https://openalex.org/", "")
        if central_paper not in ids:
            cent_papers = fetch_works_by_ids(
                [central_paper],
                save_raw_json=save_raw_json,
                raw_tag="keyword_central_paper",
                project_root=PROJECT_ROOT,
            )
            if cent_papers:
                print("Central Paper found")
                cent_rdd = sc.parallelize(cent_papers)
                cent_df = spark.read.schema(schema).json(cent_rdd.map(json.dumps))
                if cent_df.count() > 0:
                    df_seed = df_seed.unionByName(cent_df).dropDuplicates(["paperId"])
                    ids.append(central_paper)

    # 4) vertices
    vertices = (
        df_seed
        .select("paperId", "title", "year", "authors", "referenced_works", "cited_by_count")
        .withColumnRenamed("paperId", "id")
        .dropDuplicates(["id"])
    )

    # 5) edges - citation-based
    if central_paper and central_paper in ids:
        edges = build_star_edges(vertices, df_seed, central_paper)
    else:
        edges = build_citation_edges(vertices, df_seed)
        if edges.limit(1).count() == 0:
            print("No citation edges found in the seed set; building fallback edges.")
            edges = build_fallback_edges(vertices)

    # 6) GraphFrame
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
    g = GraphFrame(vertices, edges)
    g.vertices.cache()
    g.edges.cache()
    print(f"Graph: {g.vertices.count()} vertices, {g.edges.count()} edges")
    return g


def build_star_edges(vertices, df_seed, central_paper):
    """
    Build directed citation edges with central paper as hub:
    1) seed_cits: directed citation edges A→B among seed (excluding central)
    2) forward/back: directed edges between central & seed when a real citation exists
    3) fallback: central→seed for any seed not connected via citations
    4) restrict all edges to IDs in `vertices`
    """
    # 1) explode all A→B citations in seed
    all_cits = (
        df_seed
        .select(col("paperId").alias("src"), explode("referenced_works").alias("dst"))
        .dropDuplicates(["src", "dst"])
    )

    # a) directed citation edges among the other seed papers
    seed_cits = (
        all_cits
        .filter((col("src") != central_paper) & (col("dst") != central_paper))
        .select("src", "dst")
        .withColumn("edge_type", lit("seed_cite"))
    )

    # b) central→other when central cites other
    forward = (
        all_cits
        .filter(col("src") == central_paper)
        .select(lit(central_paper).alias("src"), col("dst"))
        .withColumn("edge_type", lit("central_cites"))
    )

    # c) other→central when other cites central
    backward = (
        all_cits
        .filter(col("dst") == central_paper)
        .select(col("src"), lit(central_paper).alias("dst"))
        .withColumn("edge_type", lit("cites_central"))
    )

    # 2) fallback: any seed paper not connected to central
    others = vertices.select(col("id").alias("nid")).filter(col("nid") != central_paper)
    connected = (
        forward.select("dst").withColumnRenamed("dst", "nid")
        .union(backward.select("src").withColumnRenamed("src", "nid"))
        .distinct()
    )
    unconnected = others.join(connected, on="nid", how="left_anti")
    fallback = (
        unconnected
        .select(lit(central_paper).alias("src"), col("nid").alias("dst"))
        .withColumn("edge_type", lit("fallback"))
    )

    # 3) union all raw edges
    raw = seed_cits.union(forward).union(backward).union(fallback)

    # 4) restrict to vertices set on both ends
    valid = vertices.select(col("id").alias("vid"))
    raw = (
        raw
        .join(valid.withColumnRenamed("vid", "src"), on="src")
        .join(valid.withColumnRenamed("vid", "dst"), on="dst")
    )

    # 5) add uniform weight
    edges = raw.withColumn("weight", lit(1.0)).select("src", "dst", "weight", "edge_type")
    return edges


def build_citation_edges(vertices, df_seed):
    """
    Build directed 1-hop citation edges A→B for any A in seed whose references
    include B, keeping only B's that are in our vertex set.
    """
    refs = (
        df_seed
        .select(col("paperId").alias("src"), explode("referenced_works").alias("dst"))
        .dropDuplicates(["src", "dst"])
    )
    # keep only those dst in our vertices
    refs = refs.join(
        vertices.select(col("id").alias("dst")),
        on="dst", how="inner"
    )
    edges = (
        refs
        .withColumn("weight", lit(1.0))
        .withColumn("edge_type", lit("citation"))
        .select("src", "dst", "weight", "edge_type")
    )
    return edges


def build_fallback_edges(vertices):
    """Build star-topology edges from first vertex to all others as fallback."""
    ids = [r.id for r in vertices.select("id").collect()]
    if len(ids) <= 1:
        schema = StructType([
            StructField("src", StringType(), False),
            StructField("dst", StringType(), False),
            StructField("weight", DoubleType(), False),
            StructField("edge_type", StringType(), True)
        ])
        return vertices.sparkSession.createDataFrame([], schema)
    data = []
    for other in ids[1:]:
        data += [(ids[0], other, 1.0, "fallback"), (other, ids[0], 1.0, "fallback")]
    schema = StructType([
        StructField("src", StringType(), False),
        StructField("dst", StringType(), False),
        StructField("weight", DoubleType(), False),
        StructField("edge_type", StringType(), True)
    ])
    return vertices.sparkSession.createDataFrame(data, schema)


# ─── 6. LOUVAIN COMMUNITY DETECTION ──────────────────────────────────────────

def compute_louvain_communities(g, resolution=1.0):
    """
    Run Louvain community detection on the GraphFrame citation network.
    Uses networkx + community (python-louvain) for modularity-based detection.
    Falls back to GraphFrames labelPropagation if networkx is unavailable.

    Returns a Spark DataFrame with columns: id, community
    """
    try:
        import networkx as nx
        from community import community_louvain

        # Collect edges and build networkx graph
        edges_collected = g.edges.select("src", "dst", "weight").collect()
        vertices_collected = g.vertices.select("id").collect()

        # Build undirected graph for Louvain (standard approach)
        G = nx.Graph()
        for v in vertices_collected:
            G.add_node(v.id)
        for e in edges_collected:
            if G.has_edge(e.src, e.dst):
                # aggregate weight for multi-edges
                G[e.src][e.dst]["weight"] += e.weight
            else:
                G.add_edge(e.src, e.dst, weight=e.weight)

        # Run Louvain
        partition = community_louvain.best_partition(
            G, weight="weight", resolution=resolution, random_state=42
        )

        n_communities = len(set(partition.values()))
        modularity = community_louvain.modularity(partition, G, weight="weight")
        print(f"Louvain detected {n_communities} communities (modularity={modularity:.4f})")

        # Convert to Spark DataFrame
        community_data = [(node, int(comm)) for node, comm in partition.items()]
        schema = StructType([
            StructField("id", StringType(), False),
            StructField("community", IntegerType(), False)
        ])
        spark = g.vertices.sparkSession
        community_df = spark.createDataFrame(community_data, schema)

        # Join with vertex info
        result = community_df.join(g.vertices.select("id", "title"), on="id")
        return result

    except ImportError:
        print("networkx/python-louvain not available, falling back to LabelPropagation.")
        return _label_propagation_fallback(g)


def _label_propagation_fallback(g):
    """Fallback: use GraphFrames labelPropagation as community proxy."""
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
    """Run weighted PageRank (DataFrame-based) on GraphFrame g."""
    ranks = g.vertices.select("id").withColumn("rank", lit(1.0))
    for i in range(max_iter):
        contribs = (
            g.edges
             .join(ranks.withColumnRenamed("id", "src"), on="src")
             .withColumn("contrib", col("rank") * col("weight"))
             .select(col("dst").alias("id"), "contrib")
        )
        summed = contribs.groupBy("id").sum("contrib").withColumnRenamed("sum(contrib)", "sumContrib")
        ranks = (
            g.vertices.select("id")
             .join(summed, on="id", how="left")
             .withColumn("rank", lit(reset_prob) + (1 - reset_prob) * coalesce(col("sumContrib"), lit(0.0)))
             .select("id", "rank")
        )
    return ranks.join(g.vertices.select("id", "title"), on="id")


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
    """Map a PageRank value to a hex color (light blue → dark blue)."""
    if max_rank == min_rank:
        t = 0.5
    else:
        t = (rank - min_rank) / (max_rank - min_rank)
    r = int(220 - t * 180)
    g = int(235 - t * 135)
    b = int(255 - t * 55)
    return f"#{r:02x}{g:02x}{b:02x}"


_EDGE_COLORS = {
    "citation":      "rgba(90, 130, 180, 0.4)",
    "central_cites": "rgba(40, 120, 200, 0.6)",
    "cites_central": "rgba(40, 120, 200, 0.6)",
    "seed_cite":     "rgba(90, 130, 180, 0.4)",
    "fallback":      "rgba(160, 160, 160, 0.25)",
}

_COMMUNITY_COLORS = [
    "#e6194b", "#3cb44b", "#ffe119", "#4363d8", "#f58231",
    "#911eb4", "#42d4f4", "#f032e6", "#bfef45", "#fabed4",
    "#469990", "#dcbeff", "#9a6324", "#fffac8", "#800000",
    "#aaffc3", "#808000", "#ffd8b1", "#000075", "#a9a9a9",
]


def generate_interactive_graph(
    g,
    output_file,
    project_root: Path,
    height="800px",
    width="100%",
    color_by_community=True,
):
    """Render & save a PyVis interactive graph of GraphFrame g, colored by Louvain community."""
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
        edge_type = e.edge_type if hasattr(e, "edge_type") else "citation"
        edge_color = _EDGE_COLORS.get(edge_type, "rgba(90,130,180,0.4)")
        thickness = 0.3 + w * 3.0
        arrows = "" if edge_type == "fallback" else "to"
        net.add_edge(e.src, e.dst,
                     value=thickness,
                     title=f"Type: {edge_type}\nWeight: {w:.3f}",
                     color=edge_color,
                     arrows=arrows)

    # ensure output folder exists
    out_dir = project_root / "output"
    out_dir.mkdir(parents=True, exist_ok=True)
    old_cwd = Path.cwd()
    os.chdir(out_dir)
    net.write_html(output_file)
    os.chdir(old_cwd)

    full_path = out_dir / output_file
    try:
        rel_path = full_path.resolve().relative_to(Path.cwd().resolve())
    except ValueError:
        rel_path = full_path.resolve()
    display(IFrame(src=str(rel_path), width=width, height=height))
    print(f"Saved interactive graph to {full_path}")
    return str(full_path)


# ─── 9. SAVE/LOAD ─────────────────────────────────────────────────────────────

def save_graph_parquet(g, name, project_root: Path, shuffle_partitions: int = 32):
    """Persist vertices & edges as Parquet under project_root/processed/."""
    out_verts = project_root / "processed" / f"{name}_vertices"
    out_edges = project_root / "processed" / f"{name}_edges_weighted"
    g.vertices \
     .write \
     .mode("overwrite") \
     .partitionBy("year") \
     .parquet(str(out_verts))
    g.edges \
     .repartition(shuffle_partitions) \
     .write \
     .mode("overwrite") \
     .parquet(str(out_edges))
    print(f"Vertices → {out_verts}, edges → {out_edges}")
    return str(out_verts), str(out_edges)


# ─── 10. OPTIONAL UI FUNCTIONS ────────────────────────────────────────────────

def search_papers_widget():
    """Display widget to search keywords (calls keyword_search)."""
    kw = widgets.Text(description="Keywords:")
    lim = widgets.IntSlider(description="Max Papers:", min=10, max=1000, step=10, value=100)
    btn = widgets.Button(description="Search", button_style="primary")
    out = widgets.Output()

    def on_click(b):
        with out:
            out.clear_output()
            terms = [t.strip() for t in kw.value.split(",")]
            papers = keyword_search(terms, limit=lim.value)
            for i, p in enumerate(papers[:5], 1):
                print(f"{i}. {p.get('title')} ({p.get('year')})\n   ID={p.get('paperId')}\n")
            if len(papers) > 5:
                print(f"... and {len(papers) - 5} more")
    btn.on_click(on_click)
    display(widgets.VBox([kw, lim, btn, out]))


def build_graph_widget(spark, sc):
    """Display widget to build & visualize a keyword-based citation graph."""
    kw = widgets.Text(description="Keywords:")
    lim = widgets.IntSlider(description="Max Papers:", min=10, max=1000, step=10, value=100)
    cent = widgets.Text(description="Base Paper:")
    raw = widgets.Checkbox(description="Save raw JSON", value=True)
    btn = widgets.Button(description="Build Graph", button_style="primary")
    out = widgets.Output()

    def on_click(b):
        with out:
            out.clear_output()
            terms = [t.strip() for t in kw.value.split(",")]
            g = build_graph_from_keywords(spark, sc, terms, limit=lim.value,
                                          central_paper=cent.value or None,
                                          save_raw_json=raw.value)
            if g:
                pr = compute_pagerank(g).orderBy(col("rank").desc()).limit(10)
                pr.show(truncate=False)
                authors = compute_author_influence(g, top_n=10)
                if authors is not None:
                    print("\nTop authors by influence:")
                    authors.show(truncate=False)
                save_graph_parquet(g, "kw_graph", PROJECT_ROOT)
                generate_interactive_graph(g, "kw_graph.html", PROJECT_ROOT)
    btn.on_click(on_click)
    display(widgets.VBox([kw, lim, cent, raw, btn, out]))

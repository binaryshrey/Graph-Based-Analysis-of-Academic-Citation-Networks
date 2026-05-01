# publication_search_module.py

import os
import time
import json
import math
import requests
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, lit, coalesce
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
    """Spark schema matching Semantic Scholar JSON (with venue + embedding)."""
    return StructType([
        StructField("paperId", StringType(), False),
        StructField("title",   StringType(), True),
        StructField("venue",   StringType(), True),
        StructField("year",    IntegerType(), True),
        StructField("embedding", StructType([
            StructField("model",  StringType(), True),
            StructField("vector", ArrayType(DoubleType()), True)
        ]), True)
    ])


# ─── 3. PUBLICATION‐YEAR SEARCH ────────────────────────────────────────────────

API_BASE = "https://api.semanticscholar.org/graph/v1"
SEARCH_FIELDS = "paperId,title,venue,year,embedding"


def publication_search(publication: str, year: int,
                       limit: int = 100, offset: int = 0):
    """
    Use /paper/search to find papers matching `publication` and `year`,
    then filter by exact year and venue substring.
    """
    params = {
        "query": f"{publication} {year}",
        "fields": SEARCH_FIELDS,
        "limit": min(limit, 100),
        "offset": offset
    }

    api_key = "s2k-ggzwNeWClIa8x62J17JKhjb28bApxfHdYxF7A63v"
    headers = {"x-api-key": api_key}
    
    url = f"{API_BASE}/paper/search"
    session = requests.Session()
    r = session.get(url, params=params, headers=headers)
    r.raise_for_status()
    data = r.json().get("data", [])
    # filter exact year and a loose venue match
    return [
        p for p in data
        if p.get("year")==year and (p.get("venue") or "").lower().find(publication.lower())>=0
    ]


# ─── 4. COSINE SIMILARITY UDF ─────────────────────────────────────────────────

def calculate_cosine_similarity(a, b):
    """Safe cosine similarity for two Python lists."""
    if not a or not b:
        return 0.0
    dot = sum(x*y for x,y in zip(a,b))
    na  = math.sqrt(sum(x*x for x in a))
    nb  = math.sqrt(sum(y*y for y in b))
    return float(dot/(na*nb)) if na and nb else 0.0


# ─── 5. GRAPH BUILDER ────────────────────────────────────────────────────────

def build_graph_from_publication(
    spark, sc,
    publication: str, year: int,
    limit: int = 100,
    min_similarity: float = 0.0
):
    schema = define_schema()
    cos_udf = udf(calculate_cosine_similarity, DoubleType())

    # ─── 1) fetch papers ───────────────────────────────────────────
    papers = publication_search(publication, year, limit=limit)
    print(f"🔍 Papers fetched: {len(papers)}")          # ← debug #1
    if len(papers) < 2:
        print("Need at least 2 papers to build edges.")
        return None

    # ─── 2) build vertices DF ───────────────────────────────────
    rdd = sc.parallelize(papers)
    vertices = (
        spark.read.schema(schema)
             .json(rdd.map(json.dumps))
             .dropDuplicates(["paperId"])
             .withColumnRenamed("paperId","id")
             .select("id","title","venue","year","embedding.vector")
    )
    v_count = vertices.count()
    print(f"📊 Vertices built: {v_count}")            # ← debug #2
    if v_count < 2:
        print("Not enough distinct IDs to form edges.")
        return None

    # ─── 3) build candidate pairs ────────────────────────────────
    v1 = vertices.select(col("id").alias("src"), col("vector").alias("emb_src"))
    v2 = vertices.select(col("id").alias("dst"), col("vector").alias("emb_dst"))
    # you can use join(v1.src < v2.dst) instead of crossJoin+filter if you like
    raw = v1.crossJoin(v2).filter(col("src") < col("dst"))
    pair_count = raw.count()
    print(f"🔗 Candidate pairs: {pair_count}")        # ← debug #3
    if pair_count == 0:
        print("Your < condition never matched any (src,dst) pairs.")
        return None

    # ─── 4) compute & filter by similarity ───────────────────────
    edges = (
        raw.withColumn("weight", cos_udf("emb_src","emb_dst"))
           .filter(col("weight") >= min_similarity)
           .select("src","dst","weight")
    )
    e_count = edges.count()
    print(f"✂️ Edges after threshold: {e_count}")      # ← debug #4
    if e_count == 0:
        print("No edges survived your similarity threshold.")
        return None

    # ─── 5) make directed (optional) ─────────────────────────────
    edges_dir = edges.union(
        edges.select(col("dst").alias("src"),
                     col("src").alias("dst"),
                     col("weight"))
    )

    # ─── 6) assemble GraphFrame ─────────────────────────────────
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
        vertices.select("id","title","venue","year"),
        edges_dir
    )
    g.vertices.cache()
    g.edges.cache()

    print(f"✅ Built graph with {g.vertices.count()} nodes "
          f"and {g.edges.count()} edges")
    return g


# ─── 6. PAGERANK ───────────────────────────────────────────────────────────────

def compute_pagerank(g, reset_prob: float = 0.15, max_iter: int = 10):
    """Run weighted PageRank (DataFrame‐based) on GraphFrame g."""
    ranks = g.vertices.select("id").withColumn("rank", lit(1.0))
    for _ in range(max_iter):
        contribs = (g.edges
            .join(ranks.withColumnRenamed("id","src"), on="src")
            .withColumn("contrib", col("rank")*col("weight"))
            .select(col("dst").alias("id"), "contrib")
        )
        summed = contribs.groupBy("id") \
                         .sum("contrib") \
                         .withColumnRenamed("sum(contrib)","sumContrib")
        ranks = (g.vertices.select("id")
            .join(summed, on="id", how="left")
            .withColumn("rank", lit(reset_prob) +
                                  (1-reset_prob)*coalesce(col("sumContrib"), lit(0.0)))
            .select("id","rank")
        )
    return ranks.join(g.vertices.select("id","title","venue","year"), on="id")


# ─── 7. VISUALIZATION ──────────────────────────────────────────────────────────

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


def generate_interactive_graph(
    g,
    output_file: str,
    project_root: Path,
    height: str = "800px",
    width: str  = "100%",
):
    """Use PyVis to render & save an interactive similarity graph."""
    pr = compute_pagerank(g).orderBy(col("rank").desc()).limit(50).collect()
    rank_map = {r.id: r.rank for r in pr}
    top_ids = {r.id for r in pr[:10]}
    ranks = list(rank_map.values())
    min_r, max_r = (min(ranks), max(ranks)) if ranks else (0, 1)

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
        tooltip = f"{v.title or 'Untitled'}\nVenue: {v.venue}\nYear: {v.year}\nPageRank: {rank:.4f}"
        net.add_node(v.id,
                     label=label,
                     title=tooltip,
                     size=size,
                     color=_rank_color(rank, min_r, max_r),
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


# ─── 8. WIDGET ────────────────────────────────────────────────────────────────

def build_publication_graph_widget(spark, sc):
    """ipywidget UI: input publication & year → build & show similarity graph."""
    pub = widgets.Text(description="Publication:", placeholder="e.g. Nature")
    yr  = widgets.IntText(description="Year:", value=2020)
    lim = widgets.IntSlider(description="Max Papers:", min=10, max=200,
                            step=10, value=100)
    sim = widgets.FloatSlider(description="Min Sim:", min=0.0, max=1.0,
                              step=0.05, value=0.2)
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
                min_similarity=sim.value
            )
            if g:
                html = generate_interactive_graph(
                    g, "pub_graph.html", PROJECT_ROOT
                )
                print(f"Graph saved to {html}")

    btn.on_click(on_click)
    display(widgets.VBox([pub, yr, lim, sim, btn, out]))

# Big Data | CS-GY 6513 C

## Graph-Based Analysis of Academic Citation Networks

### Team Members

| Name              | NetID   |
| ----------------- | ------- |
| Navya Gupta       | NG3118  |
| Hardik Amarwani   | HA3290  |
| Shreyansh Saurabh | SS21034 |

---

## Abstract

A scalable big-data framework built on Apache Spark and GraphFrames for uncovering thematic research communities in large-scale academic citation networks. The project ingests scholarly metadata from **OpenAlex** (scoped to New York University affiliations), constructs a directed citation graph, applies the **Louvain Community Detection** algorithm to identify clusters of related research papers, and uses **weighted PageRank** to surface the most influential publications. Interactive PyVis visualizations color nodes by detected community, enabling researchers to visually explore how different research topics interconnect within NYU's scholarly output.

---

## Dataset

### Source

**OpenAlex** (https://openalex.org/) — an open and comprehensive catalog of hundreds of millions of scholarly works, authors, institutions, and citation relationships.

### Subset

Rather than processing the full OpenAlex dataset (>1 TB), this project focuses on research papers affiliated with **New York University**, accessible via:

```
https://api.openalex.org/i57206974
```

- **Direct NYU papers**: ~266,200 works
- **Expanded citation network** (papers connected through citation links): ~25,300,000 works

### Attributes Retrieved

| Attribute             | Description                                      |
| --------------------- | ------------------------------------------------ |
| Paper ID              | OpenAlex work ID (e.g., `W2741809807`)           |
| Title                 | Title of the publication                         |
| Publication Year      | Year of publication                              |
| Authors               | Author names and OpenAlex author IDs             |
| Institutional Affiliations | Author-institution links                   |
| Referenced Works      | Directed citation relationships (paper A cites B)|
| Cited-by Count        | Number of incoming citations                     |
| Primary Location      | Source/venue (journal, conference, etc.)          |

### API Access

Data is retrieved programmatically using the OpenAlex REST API with:
- **Institution filter**: `authorships.institutions.lineage:i57206974`
- **Authentication**: API key passed as `api_key` query parameter
- **Pagination**: `per_page` (max 100) + `page` / `cursor` for deep pagination
- **Batch lookups**: OR-filter syntax (`openalex_id:W123|W456|...`) for up to 50 IDs per request

---

## Graph Analytics

### Citation Network Model

- **Nodes** = research papers (with metadata: title, year, venue, authors)
- **Edges** = directed citation links (paper A references paper B), weight = 1.0

### Louvain Community Detection

The primary analytics method. The Louvain algorithm optimizes modularity to partition the citation graph into communities of densely interconnected papers. These communities represent:
- Underlying research topics
- Thematic groupings
- Knowledge communities within NYU-affiliated publications

**Implementation**: Uses `networkx` + `python-louvain` (`community.community_louvain.best_partition`) with configurable resolution parameter. Falls back to GraphFrames `labelPropagation` if dependencies are unavailable.

**Output**: Each paper is assigned a community ID. The visualization colors nodes by community, making cluster structure immediately visible.

### Weighted PageRank

A custom Spark DataFrame-based PageRank implementation identifies the most influential papers based on citation connectivity. Configurable parameters:
- Reset probability (default: 0.15)
- Max iterations (default: 10)

Node size in visualizations is proportional to PageRank score.

---

## Architecture

### Technology Stack

| Layer         | Component              | Technology                              |
| ------------- | ---------------------- | --------------------------------------- |
| Ingestion     | API Client + Paginator | Python `requests` / OpenAlex REST API   |
| Storage       | Distributed Store      | Parquet via PyArrow / Local FS / HDFS   |
| Processing    | Spark Session          | Apache PySpark (distributed DataFrames) |
| Graph         | GraphFrames            | Spark + GraphFrames library             |
| Analytics     | Community Detection    | Louvain Algorithm (python-louvain)      |
| Analytics     | Centrality             | Weighted PageRank (custom Spark impl)   |
| Orchestration | Notebook               | Jupyter (`driver.ipynb`)                |
| Visualization | Graph Renderer         | PyVis 0.3.1 (interactive HTML)          |

### Data Flow

1. User submits a query (keyword / paper ID / publication+year) via Jupyter widget
2. The relevant module calls the OpenAlex API with NYU institution filter
3. Raw JSON responses are parsed, normalized, and deduplicated
4. Spark reads the data into DataFrames
5. Two DataFrames are built: **vertices** (papers) and **edges** (citations)
6. GraphFrames constructs the directed citation graph
7. Louvain community detection partitions nodes into research clusters
8. PageRank scores identify influential papers
9. PyVis renders an interactive HTML visualization (nodes colored by community, sized by PageRank)
10. User explores the graph in their browser

---

## Repository Structure

```
.
├── driver.ipynb                    # Jupyter notebook orchestrating all workflows
├── modules/                        # Core Python modules
│   ├── keyword_search_module.py    # Search by keywords, build citation graph
│   ├── paper_id_search_module.py   # Fetch by OpenAlex IDs, expand 1-hop citations
│   └── publication_search_module.py # Search by venue/publication name + year
├── keyword_search/                 # Keyword search artifacts
│   ├── checkpoints/                # Spark checkpointing
│   └── output/                     # Generated HTML visualizations
├── paper_id_search/                # Paper-ID search artifacts
│   ├── checkpoints/
│   ├── output/
│   └── processed/                  # Parquet storage (vertices + edges)
├── publication_search/             # Publication search artifacts
│   └── output/
├── lib/                            # Shared utilities
├── requirements.txt                # Python dependencies
├── venv/                           # Virtual environment
└── README.md                       # This file
```

---

## Module Details

### `keyword_search_module.py`

Searches OpenAlex for NYU-affiliated papers matching keyword queries. Supports:
- Free-text keyword search across paper titles
- Optional "central paper" mode (star topology with a hub paper)
- Citation-based edge construction (A cites B within the result set)
- Fallback star-topology edges when no intra-set citations exist
- Louvain community detection + PageRank + interactive visualization

**Key functions**:
- `keyword_search(keywords, limit)` — query OpenAlex
- `build_graph_from_keywords(spark, sc, keywords, ...)` — full pipeline
- `compute_louvain_communities(g)` — detect research clusters
- `compute_pagerank(g)` — rank papers by influence
- `generate_interactive_graph(g, ...)` — PyVis HTML output

### `paper_id_search_module.py`

Builds a citation graph starting from specific OpenAlex work IDs:
- Fetches seed paper metadata via batch OR-filter lookup
- Expands 1-hop: retrieves all referenced papers (up to 200)
- Constructs directed citation edges between papers in the set
- Persists results as Parquet (vertices partitioned by year, edges repartitioned)

**Key functions**:
- `fetch_works_by_ids(openalex_ids)` — batch API lookup
- `build_graph_from_ids(spark, sc, seed_ids)` — full pipeline
- `save_graph_parquet(g, name, project_root)` — persist to Parquet

### `publication_search_module.py`

Searches for NYU papers published in a specific venue/source during a given year:
- Combines institution filter + year filter + text search on venue name
- Builds citation edges between papers in the result set
- Falls back to bidirectional connectivity edges if no intra-set citations exist

**Key functions**:
- `publication_search(publication, year, limit)` — query OpenAlex
- `build_graph_from_publication(spark, sc, publication, year, ...)` — full pipeline

---

## Installation & Setup

### 1. Clone the repository

```bash
git clone https://github.com/binaryshrey/Graph-Based-Analysis-of-Academic-Citation-Networks.git
cd Graph-Based-Analysis-of-Academic-Citation-Networks
```

### 2. Create a Python virtual environment

```bash
python3 -m venv venv
source venv/bin/activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

Or manually:

```bash
pip install \
  pyspark \
  graphframes \
  pyarrow \
  ipywidgets \
  pyvis==0.3.1 \
  networkx \
  python-louvain \
  matplotlib \
  requests
```

### 4. Verify Java (required for Spark)

```bash
java -version   # Requires Java 8, 11, or 17
```

---

## Usage

Launch the driver notebook:

```bash
jupyter notebook driver.ipynb
```

### Initialize Spark

```python
from keyword_search_module import initialize_spark
spark, sc = initialize_spark(driver_memory="6g", shuffle_partitions=32)
```

### Distribute Modules to Executors

```python
sc.addPyFile("modules/keyword_search_module.py")
sc.addPyFile("modules/publication_search_module.py")
sc.addPyFile("modules/paper_id_search_module.py")
```

### Set Project Roots

```python
import keyword_search_module as ksm
import publication_search_module as psm
import paper_id_search_module as pidm

ksm.PROJECT_ROOT = PROJECT_ROOT / "keyword_search"
psm.PROJECT_ROOT = PROJECT_ROOT / "publication_search"
pidm.PROJECT_ROOT = PROJECT_ROOT / "paper_id_search"
```

### Run Workflows

**Keyword-based Search** — find papers by topic and visualize citation clusters:
```python
from keyword_search_module import build_graph_widget
build_graph_widget(spark, sc)
```

**Paper-ID Search** — explore citation neighborhood of specific papers:
```python
from paper_id_search_module import build_id_graph_widget
build_id_graph_widget(spark, sc)
```

**Publication-Year Search** — analyze papers from a specific venue/year:
```python
from publication_search_module import build_publication_graph_widget
build_publication_graph_widget(spark, sc)
```

### Programmatic Usage (without widgets)

```python
from keyword_search_module import (
    build_graph_from_keywords,
    compute_louvain_communities,
    compute_pagerank,
    get_community_summary,
    generate_interactive_graph
)

# Build graph
g = build_graph_from_keywords(spark, sc, ["deep learning"], limit=100)

# Detect communities
communities = compute_louvain_communities(g, resolution=1.0)
summary = get_community_summary(communities)
summary.show(truncate=False)

# Rank papers
pagerank = compute_pagerank(g)
pagerank.orderBy("rank", ascending=False).show(10)

# Visualize
generate_interactive_graph(g, "output.html", PROJECT_ROOT)
```

---

## Dependencies

| Package        | Version | Purpose                                    |
| -------------- | ------- | ------------------------------------------ |
| pyspark        | 4.1.1   | Distributed data processing                |
| graphframes    | 0.6     | Spark-based graph construction             |
| pyarrow        | 24.0.0  | Parquet I/O                                |
| networkx       | 3.6.1   | Graph algorithms (Louvain)                 |
| python-louvain | 0.16    | Louvain community detection                |
| pyvis          | 0.3.1   | Interactive network visualization          |
| ipywidgets     | 8.1.8   | Jupyter widget UI                          |
| matplotlib     | 3.10.9  | Supplementary plotting                     |
| requests       | -       | HTTP client for OpenAlex API               |

---

## Output Examples

The visualization generates interactive HTML files where:
- **Node color** = Louvain community assignment (20 distinct colors)
- **Node size** = PageRank score (larger = more influential)
- **Edges** = directed citation links
- **Hover tooltips** = paper title, year, community ID, PageRank score
- **Physics simulation** = ForceAtlas2-based layout for natural clustering

Output files are saved to `<workflow>/output/` directories (e.g., `keyword_search/output/kw_graph.html`).

---

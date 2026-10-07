"""
Member 1 — Sample Text Datasets for Real-Time Stream Generation.
Provides realistic textual templates and dynamic variations across 6 core categories:
- Technology
- Big Data
- Artificial Intelligence
- Data Science
- News
- Product Reviews
"""

import random
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Optional

CATEGORIES: List[str] = [
    "Technology",
    "Big Data",
    "Artificial Intelligence",
    "Data Science",
    "News",
    "Product Reviews",
]

CATEGORY_TEMPLATES: Dict[str, List[str]] = {
    "Technology": [
        "Python is widely used for data analytics and cloud backend engineering.",
        "Kubernetes and Docker containers simplify microservices deployment across hybrid cloud clusters.",
        "Modern TypeScript and React applications deliver responsive real-time dashboards with WebSockets.",
        "Cloud native architecture leverages Rust, Go, and Python for low-latency distributed systems.",
        "PostgreSQL database indexing and connection pooling improve high-concurrency API throughput.",
        "Zero-trust cybersecurity frameworks protect distributed cloud infrastructure and API gateways.",
        "Edge computing and 5G networks reduce latency for real-time IoT telemetry pipelines.",
        "FastAPI and asynchronous Python enable high-performance REST and WebSocket microservices.",
        "Linux kernel optimizations and eBPF observability tools enhance container runtime security.",
        "GraphQL and REST APIs power scalable web applications with TypeScript and React.",
        "Serverless cloud functions scale automatically during high-traffic production workloads.",
        "Distributed tracing and OpenTelemetry monitor latency across microservices and Kafka brokers.",
    ],
    "Big Data": [
        "Apache Kafka supports real time data streaming and high-throughput event ingestion.",
        "Spark Structured Streaming processes large data streams with fault-tolerant checkpointing.",
        "Apache Spark DataFrames and Catalyst optimizer accelerate distributed big data analytics.",
        "Watermarking in Spark Structured Streaming handles late-arriving event data accurately.",
        "Time-window aggregations compute 1-minute and 5-minute keyword frequency analytics in Spark.",
        "Distributed stream processing pipelines ingest millions of Kafka events per second.",
        "Apache Parquet and Delta Lake provide ACID transactions for petabyte-scale data lakes.",
        "Kafka partitions and consumer groups enable scalable parallel stream ingestion in Spark.",
        "Real-time big data pipelines combine Apache Kafka, Spark Streaming, and PostgreSQL.",
        "Checkpointing ensures exactly-once or at-least-once fault recovery in Spark streaming jobs.",
        "Sliding and tumbling windows in PySpark capture real-time keyword velocity and volume.",
        "High-velocity data streams require distributed messaging brokers like Apache Kafka.",
    ],
    "Artificial Intelligence": [
        "Large language models and transformer architectures revolutionize natural language processing.",
        "Deep learning neural networks accelerate computer vision and speech recognition tasks.",
        "Retrieval augmented generation combines vector databases with generative AI models.",
        "Multi-agent AI systems coordinate complex reasoning and autonomous tool execution.",
        "GPU clusters and tensor cores optimize training throughput for foundation models.",
        "Fine-tuning transformer models with reinforcement learning improves alignment and accuracy.",
        "Real-time AI inference pipelines process streaming text for sentiment and keyword trends.",
        "Multimodal foundation models understand text, audio, code, and visual data simultaneously.",
        "Vector embeddings and semantic search power intelligent enterprise knowledge retrieval.",
        "Neural network quantization reduces memory footprint for edge AI deployment.",
        "Prompt engineering and structured output schemas ensure reliable generative AI responses.",
        "Attention mechanisms allow transformer models to capture long-range context in text.",
    ],
    "Data Science": [
        "Exploratory data analysis and feature engineering improve machine learning model accuracy.",
        "Python libraries like Pandas, Polars, and Scikit-learn streamline statistical modeling.",
        "Time-series forecasting models detect seasonal trends and anomalies in streaming metrics.",
        "Cross-validation and hyperparameter tuning prevent overfitting in classification pipelines.",
        "Statistical hypothesis testing and A/B experiments guide data-driven product decisions.",
        "Gradient boosting algorithms and random forests excel on structured tabular datasets.",
        "Dimensionality reduction with PCA and UMAP visualizes high-dimensional keyword clusters.",
        "Natural language tokenization and stop-word removal clean raw text for frequency analysis.",
        "Data scientists use PySpark and SQL to analyze massive behavioral event datasets.",
        "Precision, recall, and F1-score evaluate classification performance on imbalanced data.",
        "Bayesian inference and probabilistic programming quantify uncertainty in predictive models.",
        "Automated feature pipelines extract real-time signals from streaming user interactions.",
    ],
    "News": [
        "Breaking news: Global technology sector announces major investments in cloud and AI infrastructure.",
        "Market update: Enterprise adoption of real-time big data analytics reaches record high this quarter.",
        "Industry report highlights rapid growth in open-source streaming technologies like Kafka and Spark.",
        "Tech summit showcases breakthroughs in quantum computing, cybersecurity, and green data centers.",
        "Financial analysts report strong demand for scalable PostgreSQL databases and cloud analytics.",
        "Global developers survey names Python, TypeScript, and Rust among most admired languages.",
        "Regulatory framework introduces new standards for data privacy and transparent AI governance.",
        "Cloud providers expand regional availability zones to support low-latency streaming workloads.",
        "Startups raise venture funding to build next-generation real-time observability platforms.",
        "Universities expand curriculum in big data engineering, Apache Spark, and distributed systems.",
        "Open-source community releases performance upgrades for Apache Kafka and PySpark connectors.",
        "Enterprise leaders emphasize real-time analytics dashboards for operational decision making.",
    ],
    "Product Reviews": [
        "The real-time analytics dashboard is super fast, intuitive, and updates charts smoothly.",
        "Excellent streaming pipeline performance: Kafka and Spark handled our peak traffic effortlessly.",
        "PostgreSQL query response time is impressive and the FastAPI backend is rock solid.",
        "Great developer experience with Docker Compose setup, everything started with a single command.",
        "The live keyword trend detection helped our team spot emerging customer topics immediately.",
        "WebSocket live updates work flawlessly without needing to refresh the browser page.",
        "Stream controls for start, pause, resume, and stop are responsive and very easy to use.",
        "Highly reliable big data platform with accurate window analytics and zero downtime.",
        "The React and Recharts visualizations make complex keyword frequency data easy to understand.",
        "Very satisfied with the fault tolerance and automatic Spark checkpoint recovery features.",
        "Configurable streaming rate slider allows smooth testing from low to high message velocity.",
        "Clean API documentation and modular design made team integration straightforward and fast.",
    ],
}

DYNAMIC_MODIFIERS: List[str] = [
    "in production environments",
    "with high scalability",
    "for real-time enterprise workloads",
    "across distributed clusters",
    "with low latency",
    "using modern cloud architecture",
    "for actionable business insights",
    "with automated fault tolerance",
]


def generate_message(category: Optional[str] = "Technology", sequence_id: Optional[int] = None) -> Dict[str, object]:
    """
    Generate a realistic streaming text message payload for the specified category.
    If category is 'All' or None, selects randomly from all 6 categories.
    """
    selected_category = category or "Technology"
    if selected_category.lower() == "all" or selected_category not in CATEGORY_TEMPLATES:
        selected_category = random.choice(CATEGORIES)

    templates = CATEGORY_TEMPLATES[selected_category]
    base_text = random.choice(templates)

    # Occasionally enrich with a contextual modifier for natural variety while keeping core keywords clear
    if random.random() < 0.35:
        modifier = random.choice(DYNAMIC_MODIFIERS)
        text = f"{base_text[:-1]} {modifier}."
    else:
        text = base_text

    now_iso = datetime.now(timezone.utc).isoformat()
    return {
        "id": str(uuid.uuid4()),
        "sequence_id": sequence_id if sequence_id is not None else 0,
        "text": text,
        "category": selected_category,
        "timestamp": now_iso,
    }

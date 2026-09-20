import os
import re
import csv
import uuid
import logging
from pathlib import Path
from typing import List, Dict, Optional, Any, Union

from app.core.config import settings
from app.services.etl.chunker import LegalDocumentChunker, LegalChunk, extract_text_from_file

logger = logging.getLogger(__name__)

# Qdrant constants — keep in lockstep with the retrieve/egestion service
COLLECTION_NAME = settings.QDRANT_COLLECTION_NAME
VECTOR_DIMENSION = settings.EMBEDDING_DIMENSION
EMBEDDING_MODEL_NAME = settings.EMBEDDING_MODEL_NAME
INDEXED_PAYLOAD_FIELDS = ["doc_id", "source_file", "jurisdiction", "document_type", "act_name"]


class ETLIngestionPipeline:
    """
    Production ETL Ingestion Pipeline for IP-SAKTI Sahayak.
    Orchestrates extraction, hierarchical chunking, dense embedding generation,
    Qdrant vector upsert, and Neo4j knowledge graph construction.
    """

    def __init__(
        self,
        qdrant_url: Optional[str] = None,
        qdrant_api_key: Optional[str] = None,
        neo4j_uri: Optional[str] = None,
        neo4j_user: Optional[str] = None,
        neo4j_pass: Optional[str] = None,
        lazy_load_models: bool = True,
    ):
        self.qdrant_url = qdrant_url or settings.QDRANT_URL
        self.qdrant_api_key = qdrant_api_key or getattr(settings, "QDRANT_API_KEY", "")
        self.neo4j_uri = neo4j_uri or settings.NEO4J_URI
        self.neo4j_user = neo4j_user or settings.NEO4J_USER
        self.neo4j_pass = neo4j_pass or settings.NEO4J_PASS

        self._embedder = None
        self._qdrant_client = None
        self._neo4j_driver = None

        if not lazy_load_models:
            self._get_embedder()

    # --- Lazy Loaded Resources ---

    def _get_embedder(self):
        """Lazy loader for SentenceTransformer model."""
        if self._embedder is None:
            from sentence_transformers import SentenceTransformer
            logger.info(f"Loading embedding model: {EMBEDDING_MODEL_NAME}...")
            self._embedder = SentenceTransformer(EMBEDDING_MODEL_NAME)
        return self._embedder

    def _get_qdrant_client(self, force_cloud: bool = False):
        """Lazy loader for Qdrant client with automatic cloud support."""
        if self._qdrant_client is None:
            # 1. Try remote / cloud Qdrant
            try:
                from qdrant_client import QdrantClient
                port = 443 if self.qdrant_url.startswith("https://") else (6333 if ":6333" in self.qdrant_url else None)
                client_kwargs = {
                    "url": self.qdrant_url,
                    "timeout": 8.0,
                }
                if port:
                    client_kwargs["port"] = port
                if self.qdrant_api_key:
                    client_kwargs["api_key"] = self.qdrant_api_key

                remote_client = QdrantClient(**client_kwargs)
                remote_client.get_collections()
                self._qdrant_client = remote_client
                logger.info(f"Connected to Qdrant Cloud at {self.qdrant_url} (port {port})")
                return self._qdrant_client
            except Exception as e:
                logger.warning(f"Remote Qdrant unavailable at {self.qdrant_url} ({e}).")
                if force_cloud:
                    raise e

            # 2. Fallback to embedded local Qdrant engine (stores data locally on disk or memory)
            try:
                from qdrant_client import QdrantClient
                local_dir = Path("data/local_qdrant")
                local_dir.mkdir(parents=True, exist_ok=True)
                self._qdrant_client = QdrantClient(path=str(local_dir))
                logger.info(f"Initialized embedded local Qdrant storage at {local_dir}")
            except Exception as le:
                logger.warning(f"Embedded local disk Qdrant failed ({le}), using in-memory Qdrant instance.")
                from qdrant_client import QdrantClient
                self._qdrant_client = QdrantClient(location=":memory:")
        return self._qdrant_client

    def _get_neo4j_driver(self):
        """Lazy loader for Neo4j driver with fast connectivity test."""
        if self._neo4j_driver is None:
            try:
                from neo4j import GraphDatabase
                driver = GraphDatabase.driver(
                    self.neo4j_uri,
                    auth=(self.neo4j_user, self.neo4j_pass),
                    connection_timeout=1.0,
                )
                with driver.session() as s:
                    s.run("RETURN 1")
                self._neo4j_driver = driver
                logger.info(f"Connected to Neo4j at {self.neo4j_uri}")
            except Exception as e:
                logger.warning(f"Neo4j service unavailable at {self.neo4j_uri}: {e}. Skipping graph database writes.")
                return None
        return self._neo4j_driver

    def close(self):
        """Closes all database connections."""
        if self._neo4j_driver:
            try:
                self._neo4j_driver.close()
            except Exception:
                pass
            self._neo4j_driver = None

    # --- Qdrant Vector DB Operations ---

    def init_qdrant_collection(self) -> bool:
        """
        Initializes the Qdrant collection 'legal_chunks' with Cosine distance and 384 dimensions.
        Creates keyword payload indexes on doc_id, jurisdiction, document_type, and act_name.
        """
        client = self._get_qdrant_client()
        if client is None:
            logger.warning("[Qdrant] Service unavailable, skipping collection initialization.")
            return False

        try:
            from qdrant_client.http import models as qmodels
            # Check if collection exists
            collections_resp = client.get_collections()
            existing_names = [col.name for col in collections_resp.collections]

            if COLLECTION_NAME not in existing_names:
                logger.info(f"[Qdrant] Creating collection '{COLLECTION_NAME}' (384-dim, Cosine)...")
                client.create_collection(
                    collection_name=COLLECTION_NAME,
                    vectors_config=qmodels.VectorParams(
                        size=VECTOR_DIMENSION,
                        distance=qmodels.Distance.COSINE,
                    ),
                )
            else:
                logger.info(f"[Qdrant] Collection '{COLLECTION_NAME}' already exists.")

            # Ensure payload indexes exist
            for field in INDEXED_PAYLOAD_FIELDS:
                try:
                    client.create_payload_index(
                        collection_name=COLLECTION_NAME,
                        field_name=field,
                        field_schema=qmodels.PayloadSchemaType.KEYWORD,
                    )
                    logger.debug(f"[Qdrant] Payload index verified for field: '{field}'")
                except Exception as idx_err:
                    # Index might already exist
                    logger.debug(f"[Qdrant] Index notice for '{field}': {idx_err}")

            return True
        except Exception as e:
            logger.error(f"[Qdrant] Initialization error: {e}")
            return False

    def delete_document_vectors(
        self,
        doc_id: Optional[str] = None,
        source_files: Optional[Union[str, List[str]]] = None,
    ) -> int:
        """
        Deletes all existing points from Qdrant matching doc_id or source_files.
        Ensures that if a duplicate, updated, or re-uploaded document is ingested,
        all previous chunks for this document are purged, preventing duplicate chunks on retrieval.
        """
        client = self._get_qdrant_client()
        if client is None:
            return 0

        from qdrant_client.http import models as qmodels

        should_conditions = []
        if doc_id:
            should_conditions.append(
                qmodels.FieldCondition(key="doc_id", match=qmodels.MatchValue(value=doc_id))
            )

        if source_files:
            if isinstance(source_files, str):
                source_files = [source_files]
            for sf in source_files:
                if sf:
                    should_conditions.append(
                        qmodels.FieldCondition(key="source_file", match=qmodels.MatchValue(value=sf))
                    )
                    sf_name = Path(sf).name
                    if sf_name != sf:
                        should_conditions.append(
                            qmodels.FieldCondition(key="source_file", match=qmodels.MatchValue(value=sf_name))
                        )

        if not should_conditions:
            return 0

        delete_filter = qmodels.Filter(should=should_conditions)
        purged_count = 0
        try:
            try:
                count_res = client.count(
                    collection_name=COLLECTION_NAME,
                    count_filter=delete_filter,
                )
                purged_count = getattr(count_res, "count", 0)
                if not isinstance(purged_count, int):
                    purged_count = 0
            except Exception:
                purged_count = 0

            client.delete(
                collection_name=COLLECTION_NAME,
                points_selector=qmodels.FilterSelector(filter=delete_filter),
            )
            if purged_count > 0:
                logger.info(
                    f"[Qdrant] Purged {purged_count} previous chunk vector(s) for doc_id='{doc_id}' / file='{source_files}' to overwrite cleanly."
                )
            else:
                logger.debug(f"[Qdrant] Vector overwrite check executed for doc_id='{doc_id}'.")
            return purged_count
        except Exception as e:
            logger.warning(f"[Qdrant] Notice while deleting previous vectors: {e}")
            return 0

    def upsert_vectors(self, chunks: List[LegalChunk], batch_size: int = 64) -> int:
        """
        Embeds chunks and batch-upserts points into Qdrant with deterministic UUIDs.
        Purges any existing chunks for this document first to guarantee idempotency and no duplicates.
        Returns the number of points inserted.
        """
        if not chunks:
            return 0

        client = self._get_qdrant_client()
        if client is None:
            logger.warning("[Qdrant] Client unavailable. Skipping vector insertion.")
            return 0

        from qdrant_client.http import models as qmodels

        self.init_qdrant_collection()

        # Deduplication & Overwrite: purge previous chunks for this document before inserting
        unique_docs = {c.doc_id for c in chunks if c.doc_id}
        unique_files = {c.source_file for c in chunks if c.source_file}
        for d_id in unique_docs:
            self.delete_document_vectors(doc_id=d_id, source_files=list(unique_files))

        embedder = self._get_embedder()

        # Passage embeddings: no BGE query instruction. Retrieve side should prefix queries with
        # "Represent this sentence for searching relevant passages: " and also normalize.
        texts_to_embed = [chunk.content for chunk in chunks]
        logger.info(f"[Qdrant] Generating embeddings for {len(texts_to_embed)} chunks...")
        embeddings = embedder.encode(
            texts_to_embed,
            show_progress_bar=False,
            batch_size=batch_size,
            normalize_embeddings=True,
        ).tolist()

        points = []
        for idx, (chunk, vector) in enumerate(zip(chunks, embeddings)):
            # Deterministic UUID for idempotent upserts
            unique_seed = f"{chunk.doc_id}:{chunk.section_name}:{idx}"
            point_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, unique_seed))

            payload = {
                "text": chunk.content,
                "doc_id": chunk.doc_id,
                "act_name": chunk.act_name,
                "chapter_name": chunk.chapter_name or "General",
                "section_name": chunk.section_name,
                "section_title": chunk.section_title or chunk.section_name,
                "jurisdiction": chunk.jurisdiction,
                "document_type": chunk.document_type,
                "language": chunk.original_language,
                "source_file": chunk.source_file or "",
                "source_url": chunk.source_url or "",
            }

            points.append(
                qmodels.PointStruct(
                    id=point_id,
                    vector=vector,
                    payload=payload,
                )
            )

        # Batch upsert
        total_upserted = 0
        try:
            for i in range(0, len(points), batch_size):
                batch = points[i : i + batch_size]
                client.upsert(collection_name=COLLECTION_NAME, points=batch)
                total_upserted += len(batch)
                logger.debug(f"[Qdrant] Upserted batch {i // batch_size + 1} ({len(batch)} points)")
        except Exception as ue:
            logger.warning(f"[Qdrant] Remote upsert issue ({ue}). Retrying via embedded local Qdrant engine...")
            from qdrant_client import QdrantClient
            local_client = QdrantClient(location=":memory:")
            local_client.create_collection(
                collection_name=COLLECTION_NAME,
                vectors_config=qmodels.VectorParams(size=VECTOR_DIMENSION, distance=qmodels.Distance.COSINE),
            )
            for i in range(0, len(points), batch_size):
                batch = points[i : i + batch_size]
                local_client.upsert(collection_name=COLLECTION_NAME, points=batch)
                total_upserted += len(batch)

        logger.info(f"[Qdrant] Successfully indexed {total_upserted} vectors into '{COLLECTION_NAME}'.")
        return total_upserted

    # --- Neo4j Knowledge Graph Operations ---

    def init_neo4j_schema(self) -> bool:
        """
        Executes Cypher uniqueness constraints and pre-seeds domain ontology in Neo4j.
        """
        driver = self._get_neo4j_driver()
        if driver is None:
            logger.warning("[Neo4j] Service unavailable, skipping schema initialization.")
            return False

        constraints = [
            "CREATE CONSTRAINT IF NOT EXISTS FOR (s:Statute) REQUIRE s.id IS UNIQUE;",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (sec:Section) REQUIRE sec.id IS UNIQUE;",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (a:RegulatoryAuthority) REQUIRE a.id IS UNIQUE;",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (h:Herb) REQUIRE h.botanical_name IS UNIQUE;",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (pad:PriorArtDatabase) REQUIRE pad.id IS UNIQUE;",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (f:StatutoryForm) REQUIRE f.code IS UNIQUE;",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (c:ClassicalText) REQUIRE c.name IS UNIQUE;",
        ]

        try:
            with driver.session() as session:
                for stmt in constraints:
                    try:
                        session.run(stmt)
                    except Exception as c_err:
                        logger.debug(f"[Neo4j] Constraint notice: {c_err}")

                # Pre-seed Authorities, Prior Art, Forms, Herbs & Classical Texts
                seed_cypher = """
                MERGE (pad:PriorArtDatabase {id: "CSIR_TKDL"})
                  ON CREATE SET pad.name = "Traditional Knowledge Digital Library", pad.organization = "CSIR / Ministry of AYUSH"
                
                MERGE (ipo:RegulatoryAuthority {id: "IPO"})
                  ON CREATE SET ipo.name = "Indian Patent Office", ipo.ministry = "DPIIT"
                
                MERGE (nba:RegulatoryAuthority {id: "NBA"})
                  ON CREATE SET nba.name = "National Biodiversity Authority", nba.ministry = "MoEFCC"
                
                MERGE (f:StatutoryForm {code: "Form III"})
                  ON CREATE SET f.name = "Application for approval of NBA for obtaining IPR in or outside India"
                
                MERGE (ct1:ClassicalText {name: "Charaka Samhita"}) ON CREATE SET ct1.schedule = "First Schedule"
                MERGE (ct2:ClassicalText {name: "Sushruta Samhita"}) ON CREATE SET ct2.schedule = "First Schedule"
                MERGE (ct3:ClassicalText {name: "Astanga Hridaya"}) ON CREATE SET ct3.schedule = "First Schedule"

                MERGE (h1:Herb {botanical_name: "Curcuma longa"}) ON CREATE SET h1.common_name = "Turmeric / Haridra"
                MERGE (h2:Herb {botanical_name: "Azadirachta indica"}) ON CREATE SET h2.common_name = "Neem / Nimba"
                MERGE (h3:Herb {botanical_name: "Withania somnifera"}) ON CREATE SET h3.common_name = "Ashwagandha"

                MERGE (h1)-[:DOCUMENTED_IN]->(ct1)
                MERGE (h2)-[:DOCUMENTED_IN]->(ct2)
                MERGE (h3)-[:DOCUMENTED_IN]->(ct3)
                """
                session.run(seed_cypher)
            logger.info("[Neo4j] Schema constraints and domain seed nodes verified.")
            return True
        except Exception as e:
            logger.error(f"[Neo4j] Schema init error: {e}")
            return False

    def insert_graph_nodes_and_edges(self, chunks: List[LegalChunk]) -> Dict[str, int]:
        """
        Constructs the statutory hierarchy and regulatory relationships in Neo4j.
        Ontology:
        (:Statute)-[:HAS_CHAPTER]->(:Chapter)-[:CONTAINS_SECTION]->(:Section)
        (:Statute)-[:CONTAINS_SECTION]->(:Section)
        (:Section)-[:CROSS_REFERENCES_PRIOR_ART]->(:PriorArtDatabase)
        (:Section)-[:ENFORCED_BY]->(:RegulatoryAuthority)
        (:Section)-[:MANDATES_COMPLIANCE_WITH]->(:Section)
        (:Section)-[:REQUIRES_STATUTORY_FORM]->(:StatutoryForm)
        """
        if not chunks:
            return {"nodes_created": 0, "edges_created": 0}

        driver = self._get_neo4j_driver()
        if driver is None:
            logger.warning("[Neo4j] Driver unavailable. Skipping graph insertion.")
            return {"nodes_created": 0, "edges_created": 0}

        self.init_neo4j_schema()

        # Group chunks by doc_id to handle both single Acts and multi-Act compendiums
        chunks_by_doc: Dict[str, List[LegalChunk]] = {}
        for c in chunks:
            chunks_by_doc.setdefault(c.doc_id, []).append(c)

        nodes_count = 0
        edges_count = 0

        try:
            with driver.session() as session:
                for doc_id, doc_chunks in chunks_by_doc.items():
                    first_chunk = doc_chunks[0]
                    act_name = first_chunk.act_name
                    jurisdiction = first_chunk.jurisdiction
                    doc_type = first_chunk.document_type
                    source_url = first_chunk.source_url or ""

                    # 0. Deduplication & Overwrite: Purge previous chapters and sections for doc_id
                    try:
                        session.run(
                            """
                            MATCH (s:Statute {id: $doc_id})
                            OPTIONAL MATCH (c:Chapter {doc_id: $doc_id})
                            OPTIONAL MATCH (sec:Section {doc_id: $doc_id})
                            DETACH DELETE c, sec
                            """,
                            doc_id=doc_id,
                        )
                        logger.debug(f"[Neo4j] Purged previous chapter/section graph nodes for doc_id='{doc_id}' to overwrite.")
                    except Exception as purge_err:
                        logger.debug(f"[Neo4j] Notice purging old graph nodes: {purge_err}")

                    # 1. Upsert Statute Node for each detected Act
                    session.run(
                        """
                        MERGE (s:Statute {id: $doc_id})
                        ON CREATE SET s.name = $act_name,
                                      s.jurisdiction = $jurisdiction,
                                      s.document_type = $document_type,
                                      s.source_url = $source_url
                        ON MATCH SET  s.name = $act_name,
                                      s.jurisdiction = $jurisdiction,
                                      s.document_type = $document_type
                        """,
                        doc_id=doc_id,
                        act_name=act_name,
                        jurisdiction=jurisdiction,
                        document_type=doc_type,
                        source_url=source_url,
                    )
                    nodes_count += 1

                    for chunk in doc_chunks:
                        ch_name = chunk.chapter_name or "General"
                        # Safe normalized identifiers
                        ch_id = f"{doc_id}_{re.sub(r'[^a-zA-Z0-9]', '_', ch_name).strip('_').lower()}"
                        sec_id = f"{doc_id}_{re.sub(r'[^a-zA-Z0-9]', '_', chunk.section_name).strip('_').lower()}"

                        # Clean snippet (take first 300 chars of verbatim content)
                        content_parts = chunk.content.split("\n", 1)
                        snippet = content_parts[1][:300] if len(content_parts) > 1 else chunk.content[:300]

                        # 2. Upsert Chapter, Section, and Statutory Tree Edges
                        session.run(
                            """
                            MERGE (s:Statute {id: $doc_id})
                            MERGE (c:Chapter {id: $ch_id})
                              ON CREATE SET c.name = $ch_name, c.act_name = $act_name, c.doc_id = $doc_id
                            MERGE (s)-[:HAS_CHAPTER]->(c)

                            MERGE (sec:Section {id: $sec_id})
                              ON CREATE SET sec.name = $sec_name,
                                            sec.title = $sec_title,
                                            sec.doc_id = $doc_id,
                                            sec.act_name = $act_name,
                                            sec.jurisdiction = $jurisdiction,
                                            sec.snippet = $snippet
                              ON MATCH SET  sec.title = $sec_title,
                                            sec.snippet = $snippet
                            
                            MERGE (c)-[:CONTAINS_SECTION]->(sec)
                            MERGE (s)-[:CONTAINS_SECTION]->(sec)
                            """,
                            doc_id=doc_id,
                            act_name=act_name,
                            ch_id=ch_id,
                            ch_name=ch_name,
                            sec_id=sec_id,
                            sec_name=chunk.section_name,
                            sec_title=chunk.section_title or chunk.section_name,
                            jurisdiction=jurisdiction,
                            snippet=snippet,
                        )
                        nodes_count += 2
                        edges_count += 3

                        # 3. Domain Regulatory & Cross-Reference Rules for IP-SAKTI Sahayak
                        sec_lower = chunk.section_name.lower()
                        content_lower = chunk.content.lower()

                        # Rule A: Traditional Knowledge / Patents Act Section 3(p)
                        if "3(p)" in sec_lower or "traditional knowledge" in content_lower:
                            session.run(
                                """
                                MATCH (sec:Section {id: $sec_id})
                                MERGE (pad:PriorArtDatabase {id: "CSIR_TKDL"})
                                MERGE (ipo:RegulatoryAuthority {id: "IPO"})
                                MERGE (bda_sec:Section {id: "in_bda_2002_sec_6"})
                                  ON CREATE SET bda_sec.name = "Section 6",
                                                bda_sec.act_name = "Biological Diversity Act, 2002",
                                                bda_sec.doc_id = "in_bda_2002"
                                MERGE (f:StatutoryForm {code: "Form III"})
                                MERGE (nba:RegulatoryAuthority {id: "NBA"})

                                MERGE (sec)-[:CROSS_REFERENCES_PRIOR_ART]->(pad)
                                MERGE (sec)-[:ENFORCED_BY]->(ipo)
                                MERGE (sec)-[:MANDATES_COMPLIANCE_WITH]->(bda_sec)
                                MERGE (bda_sec)-[:REQUIRES_STATUTORY_FORM]->(f)
                                MERGE (bda_sec)-[:ENFORCED_BY]->(nba)
                                """,
                                sec_id=sec_id,
                            )
                            edges_count += 5

                        # Rule B: Biological Diversity Act Section 6 (Application for IPR)
                        elif "section 6" in sec_lower and ("diversity" in act_name.lower() or "bda" in doc_id):
                            session.run(
                                """
                                MATCH (sec:Section {id: $sec_id})
                                MERGE (f:StatutoryForm {code: "Form III"})
                                MERGE (nba:RegulatoryAuthority {id: "NBA"})
                                MERGE (sec)-[:REQUIRES_STATUTORY_FORM]->(f)
                                MERGE (sec)-[:ENFORCED_BY]->(nba)
                                """,
                                sec_id=sec_id,
                            )
                            edges_count += 2

            logger.info(f"[Neo4j] Inserted/updated statutory graph for '{act_name}': ~{nodes_count} nodes, ~{edges_count} relationships.")
            return {"nodes_created": nodes_count, "edges_created": edges_count}
        except Exception as e:
            logger.error(f"[Neo4j] Error constructing knowledge graph: {e}")
            return {"nodes_created": nodes_count, "edges_created": edges_count}

    def delete_document_graph(self, doc_id: str) -> bool:
        """
        Removes Statute, Chapter, and Section nodes for a given doc_id from Neo4j.
        """
        driver = self._get_neo4j_driver()
        if driver is None:
            return False
        try:
            with driver.session() as session:
                session.run(
                    """
                    MATCH (s:Statute {id: $doc_id})
                    OPTIONAL MATCH (c:Chapter {doc_id: $doc_id})
                    OPTIONAL MATCH (sec:Section {doc_id: $doc_id})
                    DETACH DELETE s, c, sec
                    """,
                    doc_id=doc_id,
                )
            logger.info(f"[Neo4j] Purged document graph for doc_id='{doc_id}'.")
            return True
        except Exception as e:
            logger.warning(f"[Neo4j] Notice deleting document graph for '{doc_id}': {e}")
            return False

    # --- Reset Database Utility ---

    def reset_databases(self) -> Dict[str, bool]:
        """Clears existing Qdrant collection and Neo4j graph for fresh ingestion."""
        results = {"qdrant_reset": False, "neo4j_reset": False}

        # Reset Qdrant
        client = self._get_qdrant_client()
        if client:
            try:
                collections = [c.name for c in client.get_collections().collections]
                if COLLECTION_NAME in collections:
                    client.delete_collection(COLLECTION_NAME)
                    logger.info(f"[Qdrant] Deleted collection '{COLLECTION_NAME}'.")
                results["qdrant_reset"] = True
            except Exception as e:
                logger.error(f"[Qdrant] Reset failed: {e}")

        # Reset Neo4j
        driver = self._get_neo4j_driver()
        if driver:
            try:
                with driver.session() as session:
                    session.run("MATCH (n) DETACH DELETE n;")
                logger.info("[Neo4j] Cleared all nodes and edges from knowledge graph.")
                results["neo4j_reset"] = True
            except Exception as e:
                logger.error(f"[Neo4j] Reset failed: {e}")

        return results

    # --- Ingestion Orchestration ---

    def ingest_file(
        self,
        file_path: Union[str, Path],
        doc_metadata: Optional[Dict[str, Any]] = None,
        format_type: str = "auto",
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """
        Executes end-to-end ingestion for a single document.
        1. Extracts text (.txt or .pdf with cleaning).
        2. Chunks text into hierarchical LegalChunk objects.
        3. Upserts dense embeddings to Qdrant (unless dry_run).
        4. Injects statutory graph & relationships into Neo4j (unless dry_run).
        5. Updates status in corpus_index.csv.
        """
        path = Path(file_path).resolve()
        if not path.exists():
            raise FileNotFoundError(f"File not found: {path}")

        meta = doc_metadata or {}
        doc_id = meta.get("doc_id") or path.stem.replace(" ", "_").lower()
        act_name = meta.get("act_name") or path.stem.replace("_", " ").title()
        jurisdiction = meta.get("jurisdiction", "IN")
        doc_type = meta.get("document_type", "statute")
        source_url = meta.get("source_url") or ""
        effective_date = meta.get("effective_date")

        logger.info(f"--- Ingesting File: {path.name} [{format_type}] ---")
        logger.info(f"Metadata: Act='{act_name}', doc_id='{doc_id}', jurisdiction='{jurisdiction}'")

        # 1 & 2: Extraction and Hierarchical Chunking
        chunker = LegalDocumentChunker(
            doc_id=doc_id,
            act_name=act_name,
            jurisdiction=jurisdiction,
            effective_date=effective_date,
            document_type=doc_type,
            source_file=path.name,
            source_url=source_url,
        )

        chunks = chunker.chunk_file(path, format_type=format_type)
        logger.info(f"Generated {len(chunks)} hierarchical chunks for '{path.name}'.")

        if dry_run:
            logger.info("[DRY RUN] Skipping database insertions.")
            return {
                "file": path.name,
                "status": "Dry-run Success",
                "chunks_count": len(chunks),
                "vectors_inserted": 0,
                "graph_nodes": 0,
                "graph_edges": 0,
            }

        # 3. Vector DB Upsert
        vectors_inserted = self.upsert_vectors(chunks)

        # 4. Knowledge Graph Insertion
        graph_stats = self.insert_graph_nodes_and_edges(chunks)

        # 5. Update Status in corpus_index.csv
        self._update_corpus_index(path.name, status="Ingested")

        return {
            "file": path.name,
            "status": "Success",
            "chunks_count": len(chunks),
            "vectors_inserted": vectors_inserted,
            "graph_nodes": graph_stats.get("nodes_created", 0),
            "graph_edges": graph_stats.get("edges_created", 0),
        }

    def ingest_directory(
        self,
        dir_path: Union[str, Path],
        format_type: str = "auto",
        dry_run: bool = False,
    ) -> List[Dict[str, Any]]:
        """
        Ingests all supported documents (.txt, .md, .pdf) found in dir_path.
        """
        directory = Path(dir_path).resolve()
        if not directory.exists() or not directory.is_dir():
            raise NotADirectoryError(f"Directory not found: {directory}")

        supported_extensions = {".txt", ".md", ".pdf"}
        files_to_process = [
            f for f in directory.iterdir() if f.is_file() and f.suffix.lower() in supported_extensions
        ]

        if not files_to_process:
            logger.warning(f"No supported legal documents (.txt, .md, .pdf) found in {directory}")
            return []

        logger.info(f"Found {len(files_to_process)} document(s) in {directory}.")
        results = []
        for file in sorted(files_to_process):
            try:
                res = self.ingest_file(file, format_type=format_type, dry_run=dry_run)
                results.append(res)
            except Exception as e:
                logger.error(f"Failed to ingest {file.name}: {e}")
                self._update_corpus_index(file.name, status="Failed")
                results.append({"file": file.name, "status": f"Error: {e}", "chunks_count": 0})

        return results

    def _update_corpus_index(self, filename: str, status: str = "Ingested"):
        """
        Updates the status column in corpus_index.csv if it exists.
        """
        # Look for corpus_index.csv in common relative project paths
        candidate_paths = [
            Path("corpus_index.csv"),
            Path("../corpus_index.csv"),
            Path("../../corpus_index.csv"),
        ]
        csv_path = next((p for p in candidate_paths if p.exists()), candidate_paths[0])

        if not csv_path.exists():
            return

        try:
            rows = []
            updated = False
            with open(csv_path, mode="r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                fieldnames = reader.fieldnames or []
                for row in reader:
                    if filename in row.get("file_path", "") or filename == row.get("file_path", ""):
                        row["status"] = status
                        updated = True
                    rows.append(row)

            if updated and fieldnames:
                with open(csv_path, mode="w", newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(rows)
                logger.debug(f"Updated status for '{filename}' to '{status}' in {csv_path.resolve()}")
        except Exception as e:
            logger.warning(f"Unable to update corpus_index.csv: {e}")

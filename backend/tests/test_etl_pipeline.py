import os
import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

# Ensure backend directory is in sys.path
BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))

from app.services.etl.chunker import (
    LegalDocumentChunker,
    LegalChunk,
    clean_legal_text,
    extract_text_from_file,
)
from app.services.etl.etl_ingestion_pipeline import ETLIngestionPipeline, COLLECTION_NAME


SAMPLE_MOCK_STATUTE = """
THE GAZETTE OF INDIA : EXTRAORDINARY
[PART II—SEC. 3(i)]
PUBLISHED BY AUTHORITY
12

CHAPTER I PRELIMINARY
Section 1 Short title, extent and commencement
(1) This Act may be called the Patents Act, 1970.
(2) It extends to the whole of India.

Section 2 Definitions and interpretation
(1) In this Act, unless the context otherwise requires,—
(a) “Appellate Board” means the Appellate Board referred to in section 116;

CHAPTER II INVENTIONS NOT PATENTABLE
Section 3 What are not inventions
The following are not inventions within the meaning of this Act,—
(a) an invention which is frivolous or which claims anything obviously contrary to well established natural laws;
(d) the mere discovery of a new form of a known substance;
(p) an invention which in effect, is tra-
ditional knowledge or which is an aggregation or duplication of known properties of traditionally known component or components.
"""


def test_text_cleaner_headers_and_hyphenation():
    """Verify that headers, footers, page numbers are removed and hyphenated linebreaks are re-joined."""
    cleaned = clean_legal_text(SAMPLE_MOCK_STATUTE)

    # 1. Official headers should be stripped
    assert "THE GAZETTE OF INDIA" not in cleaned
    assert "PUBLISHED BY AUTHORITY" not in cleaned
    assert "[PART II—SEC. 3(i)]" not in cleaned

    # 2. Standalone page numbers should be stripped
    lines = [line.strip() for line in cleaned.splitlines()]
    assert "12" not in lines

    # 3. Hyphenated word split across lines should be merged
    # "tra-\nditional" -> "traditional"
    assert "traditional knowledge" in cleaned
    assert "tra-\nditional" not in cleaned


def test_hierarchical_chunker_structure_and_breadcrumbs():
    """Test chunker detects chapters, sections, sub-clauses, and injects breadcrumbs."""
    chunker = LegalDocumentChunker(
        doc_id="in_pat_1970",
        act_name="The Patents Act, 1970",
        jurisdiction="IN",
        language="en",
    )

    cleaned_text = clean_legal_text(SAMPLE_MOCK_STATUTE)
    chunks = chunker.chunk_document(cleaned_text)

    # Should have 3 section chunks: Section 1, Section 2, Section 3
    assert len(chunks) == 3

    # Check Section 1
    sec1 = chunks[0]
    assert sec1.doc_id == "in_pat_1970"
    assert sec1.act_name == "The Patents Act, 1970"
    assert sec1.chapter_name == "Chapter I PRELIMINARY"
    assert "Section 1" in sec1.section_name
    assert sec1.section_title == "Short title, extent and commencement"
    # Breadcrumb assertion
    assert "[Act: The Patents Act, 1970 | Chapter: Chapter I PRELIMINARY | Section: Section 1 Short title, extent and commencement]" in sec1.content
    assert "(1) This Act may be called the Patents Act, 1970." in sec1.content
    assert "(2) It extends to the whole of India." in sec1.content

    # Check Section 3 with sub-clauses (a), (d), (p)
    sec3 = chunks[2]
    assert sec3.chapter_name == "Chapter II INVENTIONS NOT PATENTABLE"
    assert "Section 3" in sec3.section_name
    assert sec3.section_title == "What are not inventions"
    # Sub-clauses intact
    assert "(a) an invention which is frivolous" in sec3.content
    assert "(p) an invention which in effect, is traditional knowledge" in sec3.content


def test_dual_format_file_extraction():
    """Test extraction and chunking from both .txt and .pdf files."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)

        # 1. Test .txt file
        txt_file = tmp_path / "test_act.txt"
        txt_file.write_text(SAMPLE_MOCK_STATUTE, encoding="utf-8")

        chunker = LegalDocumentChunker(doc_id="test_act", act_name="Test Act")
        txt_chunks = chunker.chunk_file(txt_file, format_type="txt")
        assert len(txt_chunks) == 3
        assert txt_chunks[0].source_file == "test_act.txt"

        # 2. Test .pdf file using PyMuPDF to generate a valid PDF
        import fitz
        pdf_path = tmp_path / "test_act.pdf"
        pdf_doc = fitz.open()
        page = pdf_doc.new_page()
        page.insert_text((50, 72), SAMPLE_MOCK_STATUTE, fontsize=10)
        pdf_doc.save(str(pdf_path))
        pdf_doc.close()

        pdf_chunks = chunker.chunk_file(pdf_path, format_type="pdf")
        assert len(pdf_chunks) >= 2  # Verify sections were detected in PDF
        assert pdf_chunks[0].source_file == "test_act.pdf"


def test_qdrant_payload_schema_compliance():
    """
    Verify that Qdrant payload exactly matches the schema expected by the egestion reader:
    text, doc_id, act_name, chapter_name, section_name, section_title, jurisdiction,
    document_type, language, source_file, source_url.
    """
    chunk = LegalChunk(
        doc_id="in_pat_1970",
        act_name="The Patents Act, 1970",
        chapter_name="Chapter II: INVENTIONS NOT PATENTABLE",
        section_name="Section 3(p)",
        section_title="Traditional Knowledge Bar",
        content="[The Patents Act, 1970 | Chapter II | Section 3(p)] The following are not inventions...",
        jurisdiction="IN",
        document_type="statute",
        original_language="en",
        source_file="patents_act_1970.txt",
        source_url="https://www.indiacode.nic.in/handle/123456789/1392",
    )

    # Mock Qdrant client to capture the generated payload
    pipeline = ETLIngestionPipeline()
    mock_client = MagicMock()
    mock_embedder = MagicMock()
    mock_embedder.encode.return_value.tolist.return_value = [[0.1] * 384]

    with patch.object(pipeline, "_get_qdrant_client", return_value=mock_client), \
         patch.object(pipeline, "_get_embedder", return_value=mock_embedder), \
         patch.object(pipeline, "init_qdrant_collection", return_value=True):

        upserted_count = pipeline.upsert_vectors([chunk])
        assert upserted_count == 1

        mock_embedder.encode.assert_called_once()
        encode_kwargs = mock_embedder.encode.call_args.kwargs
        assert encode_kwargs.get("normalize_embeddings") is True

        mock_client.upsert.assert_called_once()
        _, kwargs = mock_client.upsert.call_args
        assert kwargs["collection_name"] == COLLECTION_NAME
        points = kwargs["points"]
        assert len(points) == 1

        payload = points[0].payload
        required_keys = [
            "text", "doc_id", "act_name", "chapter_name", "section_name",
            "section_title", "jurisdiction", "document_type", "language",
            "source_file", "source_url"
        ]
        for key in required_keys:
            assert key in payload, f"Missing required payload key: {key}"

        assert payload["doc_id"] == "in_pat_1970"
        assert payload["section_name"] == "Section 3(p)"
        assert payload["section_title"] == "Traditional Knowledge Bar"
        assert payload["language"] == "en"


def test_neo4j_provenance_ontology_and_rules():
    """
    Verify Neo4j knowledge graph construction, including Section 3(p)
    relations to CSIR_TKDL, IPO, Biological Diversity Act, Form III, and NBA.
    """
    chunk_3p = LegalChunk(
        doc_id="in_pat_1970",
        act_name="The Patents Act, 1970",
        chapter_name="Chapter II",
        section_name="Section 3(p)",
        section_title="Traditional Knowledge Bar",
        content="Section 3(p) an invention which in effect, is traditional knowledge...",
        jurisdiction="IN",
    )

    pipeline = ETLIngestionPipeline()
    mock_driver = MagicMock()
    mock_session = MagicMock()
    mock_driver.session.return_value.__enter__.return_value = mock_session

    with patch.object(pipeline, "_get_neo4j_driver", return_value=mock_driver), \
         patch.object(pipeline, "init_neo4j_schema", return_value=True):

        stats = pipeline.insert_graph_nodes_and_edges([chunk_3p])
        assert stats["nodes_created"] > 0
        assert stats["edges_created"] > 0

        # Verify that Cypher statements executed include cross-references
        cypher_calls = [call[0][0] for call in mock_session.run.call_args_list]
        cypher_text = "\n".join(cypher_calls)

        assert "HAS_CHAPTER" in cypher_text
        assert "CONTAINS_SECTION" in cypher_text
        assert "CROSS_REFERENCES_PRIOR_ART" in cypher_text
        assert "CSIR_TKDL" in cypher_text
        assert "ENFORCED_BY" in cypher_text
        assert "MANDATES_COMPLIANCE_WITH" in cypher_text
        assert "Form III" in cypher_text


def test_dry_run_ingest_file():
    """Verify that ingest_file with dry_run=True extracts and chunks without database errors."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)
        sample_file = tmp_path / "sample.txt"
        sample_file.write_text("CHAPTER I\nSection 1 Title\nSample verbatim text.", encoding="utf-8")

        pipeline = ETLIngestionPipeline()
        result = pipeline.ingest_file(sample_file, dry_run=True)

        assert result["status"] == "Dry-run Success"
        assert result["chunks_count"] == 1
        assert result["vectors_inserted"] == 0


def test_complete_act_with_toc_and_bare_numbering():
    """Verify that complete Indian Acts with ARRANGEMENT OF SECTIONS and bare numbering (1. Title.—) are parsed properly."""
    raw_bare_act = """
THE PATENTS ACT, 1970
ACT NO. 39 OF 1970
[19th September, 1970]

ARRANGEMENT OF SECTIONS
CHAPTER I
PRELIMINARY
1. Short title, extent and commencement.
2. Definitions and interpretation.
CHAPTER II
INVENTIONS NOT PATENTABLE
3. What are not inventions.

An Act to amend and consolidate the law relating to patents.
BE it enacted by Parliament in the Twenty-first Year of the Republic of India as follows:—

CHAPTER I PRELIMINARY
1. Short title, extent and commencement.—(1) This Act may be called the Patents Act, 1970.
(2) It extends to the whole of India.

2. Definitions and interpretation.—(1) In this Act, unless the context otherwise requires,—
(a) “Appellate Board” means the Appellate Board;

CHAPTER II INVENTIONS NOT PATENTABLE
3. What are not inventions.—The following are not inventions within the meaning of this Act,—
(p) an invention which in effect, is traditional knowledge.
"""
    chunker = LegalDocumentChunker()
    chunks = chunker.chunk_multi_act_document(raw_bare_act)

    # Should have 4 chunks: 1 Preamble chunk + 3 Section chunks (TOC must NOT create duplicate chunks)
    assert len(chunks) == 4
    assert chunks[0].section_name == "Preamble"

    sec_chunks = [c for c in chunks if c.section_name != "Preamble"]
    assert len(sec_chunks) == 3
    assert "Section 1 Short title" in sec_chunks[0].section_name
    assert "[Act: The Patents Act, 1970 | Chapter: Chapter I PRELIMINARY | Section: Section 1 Short title" in sec_chunks[0].content
    assert "(1) This Act may be called the Patents Act, 1970." in sec_chunks[0].content
    assert "(2) It extends to the whole of India." in sec_chunks[0].content

    assert "Section 3 What are not inventions" in sec_chunks[2].section_name
    assert "(p) an invention which in effect, is traditional knowledge." in sec_chunks[2].content


def test_multi_act_compendium_auto_extraction():
    """Verify that a single compendium containing multiple distinct Acts segments each Act automatically."""
    compendium = """
THE PATENTS ACT, 1970
ACT NO. 39 OF 1970
[19th September, 1970]

CHAPTER I PRELIMINARY
Section 1 Short title
(1) This Act may be called the Patents Act, 1970.

Section 3 What are not inventions
(p) Traditional knowledge bar.

THE BIOLOGICAL DIVERSITY ACT, 2002
ACT NO. 18 OF 2003
[5th February, 2003]

CHAPTER I PRELIMINARY
Section 1 Short title
(1) This Act may be called the Biological Diversity Act, 2002.

Section 6 Application for intellectual property rights
(1) Prior approval of NBA required before applying for IPR.
"""
    chunker = LegalDocumentChunker()
    chunks = chunker.chunk_multi_act_document(compendium)

    # Should have 6 chunks: (1 Preamble + 2 Sections) for Patents Act, and (1 Preamble + 2 Sections) for Biodiversity Act
    assert len(chunks) == 6

    # Check Patents Act chunks
    pat_chunks = [c for c in chunks if "Patents Act" in c.act_name]
    assert len(pat_chunks) == 3
    assert pat_chunks[0].chapter_name == "Preamble"
    assert pat_chunks[1].doc_id == "in_the_patents_act_1970"
    assert "[Act: The Patents Act, 1970" in pat_chunks[1].content

    # Check Biodiversity Act chunks
    bda_chunks = [c for c in chunks if "Biological Diversity" in c.act_name]
    assert len(bda_chunks) == 3
    assert bda_chunks[0].chapter_name == "Preamble"
    assert bda_chunks[1].doc_id == "in_the_biological_diversity_act_2002"
    assert "[Act: The Biological Diversity Act, 2002" in bda_chunks[1].content


def test_duplicate_file_overwrite_deduplication():
    """
    Verify that re-ingesting a duplicate or modified file overwrites previous chunks
    instead of accumulating duplicates in Qdrant.
    """
    from qdrant_client import QdrantClient, models as qmodels
    from app.services.etl.etl_ingestion_pipeline import VECTOR_DIMENSION

    # Use in-memory Qdrant instance for isolated testing
    test_client = QdrantClient(location=":memory:")
    test_client.create_collection(
        collection_name=COLLECTION_NAME,
        vectors_config=qmodels.VectorParams(size=VECTOR_DIMENSION, distance=qmodels.Distance.COSINE),
    )

    pipeline = ETLIngestionPipeline()
    mock_embedder = MagicMock()
    mock_embedder.encode.return_value.tolist.return_value = [[0.05] * VECTOR_DIMENSION]

    with patch.object(pipeline, "_get_qdrant_client", return_value=test_client), \
         patch.object(pipeline, "_get_embedder", return_value=mock_embedder), \
         patch.object(pipeline, "init_qdrant_collection", return_value=True):

        # 1. First Ingestion: 3 chunks for doc_dup_test
        chunks_v1 = [
            LegalChunk(doc_id="doc_dup_test", act_name="Test Act", section_name=f"Section {i}", content=f"Text {i}", source_file="test_doc.txt")
            for i in range(1, 4)
        ]
        mock_embedder.encode.return_value.tolist.return_value = [[0.1 * i] * VECTOR_DIMENSION for i in range(3)]
        inserted_v1 = pipeline.upsert_vectors(chunks_v1)
        assert inserted_v1 == 3
        count_v1 = test_client.count(COLLECTION_NAME).count
        assert count_v1 == 3, f"Expected 3 points, got {count_v1}"

        # 2. Duplicate / Re-upload Ingestion: modified content with only 2 chunks for the SAME file
        chunks_v2 = [
            LegalChunk(doc_id="doc_dup_test", act_name="Test Act Modified", section_name=f"Section {i}", content=f"Updated Text {i}", source_file="test_doc.txt")
            for i in range(1, 3)
        ]
        mock_embedder.encode.return_value.tolist.return_value = [[0.2 * i] * VECTOR_DIMENSION for i in range(2)]
        inserted_v2 = pipeline.upsert_vectors(chunks_v2)
        assert inserted_v2 == 2

        # 3. CRITICAL: Count must be 2, NOT 5 (3 + 2)! Old chunks must be overwritten!
        count_v2 = test_client.count(COLLECTION_NAME).count
        assert count_v2 == 2, f"Expected 2 points after overwrite, got {count_v2} (duplicates were not overwritten!)"

        # Verify remaining points belong to the updated content
        points = test_client.scroll(collection_name=COLLECTION_NAME, limit=10)[0]
        assert len(points) == 2
        for pt in points:
            assert pt.payload["act_name"] == "Test Act Modified"
            assert "Updated Text" in pt.payload["text"]


if __name__ == "__main__":
    print("Running ETL Pipeline Tests...")
    test_text_cleaner_headers_and_hyphenation()
    print("  [PASS] test_text_cleaner_headers_and_hyphenation")
    test_hierarchical_chunker_structure_and_breadcrumbs()
    print("  [PASS] test_hierarchical_chunker_structure_and_breadcrumbs")
    test_dual_format_file_extraction()
    print("  [PASS] test_dual_format_file_extraction")
    test_qdrant_payload_schema_compliance()
    print("  [PASS] test_qdrant_payload_schema_compliance")
    test_neo4j_provenance_ontology_and_rules()
    print("  [PASS] test_neo4j_provenance_ontology_and_rules")
    test_dry_run_ingest_file()
    print("  [PASS] test_dry_run_ingest_file")
    test_complete_act_with_toc_and_bare_numbering()
    print("  [PASS] test_complete_act_with_toc_and_bare_numbering")
    test_multi_act_compendium_auto_extraction()
    print("  [PASS] test_multi_act_compendium_auto_extraction")
    test_duplicate_file_overwrite_deduplication()
    print("  [PASS] test_duplicate_file_overwrite_deduplication")
    print("\nALL 9 TEST CASES PASSED SUCCESSFULLY!")

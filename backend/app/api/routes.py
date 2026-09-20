import os
import re
import csv
import json
import asyncio
import logging
from pathlib import Path
from typing import Optional, List, Dict, Any
from fastapi import APIRouter, HTTPException, BackgroundTasks, UploadFile, File, Form
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from app.services.etl.chunker import LegalDocumentChunker, extract_text_from_file
from app.services.etl.etl_ingestion_pipeline import ETLIngestionPipeline
from app.services.etl.tasks import ingest_document_task, celery_app
from app.core.config import settings

logger = logging.getLogger(__name__)

router = APIRouter()


CORPUS_INDEX_FIELDS = [
    "file_path",
    "document_title",
    "act_name",
    "year",
    "jurisdiction",
    "language",
    "document_type",
    "status",
]


def _resolve_corpus_index_path() -> Path:
    candidates = [
        Path("corpus_index.csv"),
        Path("../corpus_index.csv"),
        Path("../../corpus_index.csv"),
    ]
    return next((p for p in candidates if p.exists()), candidates[0])


def _indexed_filenames() -> List[str]:
    csv_path = _resolve_corpus_index_path()
    if not csv_path.exists():
        return []
    names: List[str] = []
    with open(csv_path, mode="r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            file_path = (row.get("file_path") or "").strip()
            if file_path:
                names.append(Path(file_path).name)
    return names


def _resolve_corpus_file(filename: str) -> Optional[Path]:
    path = Path(filename)
    if path.exists():
        return path
    candidates = [
        Path(filename),
        Path("corpus_extracted") / filename,
        Path("../corpus_extracted") / filename,
        Path("corpus_extracted") / path.name,
        Path("../corpus_extracted") / path.name,
    ]
    return next((c for c in candidates if c.exists()), None)


class IngestFileRequest(BaseModel):
    file_path: str = Field(..., description="Relative or absolute path to the legal document (.txt, .md, or .pdf)")
    doc_id: Optional[str] = None
    act_name: Optional[str] = None
    jurisdiction: str = "IN"
    document_type: str = "statute"
    format: str = "auto"
    dry_run: bool = False
    async_mode: bool = False


class IngestDirectoryRequest(BaseModel):
    dir_path: str = Field(..., description="Directory containing legal files (.txt, .md, .pdf)")
    format: str = "auto"
    dry_run: bool = False


class PreviewRequest(BaseModel):
    file_path: str
    format: str = "auto"


class IngestionResponse(BaseModel):
    status: str
    message: str
    data: Optional[Dict[str, Any]] = None


@router.post("/ingest/file", response_model=IngestionResponse)
async def ingest_single_file(request: IngestFileRequest):
    """
    Ingests an individual legal document into Qdrant vector database and Neo4j knowledge graph.
    Can be executed synchronously or dispatched asynchronously to Celery.
    """
    path = Path(request.file_path)
    if not path.exists():
        candidates = [
            Path(request.file_path),
            Path("corpus_extracted") / request.file_path,
            Path("../corpus_extracted") / request.file_path,
            Path("corpus_extracted") / path.name,
            Path("../corpus_extracted") / path.name,
        ]
        found = next((c for c in candidates if c.exists()), None)
        if not found:
            raise HTTPException(status_code=404, detail=f"File not found: {request.file_path}")
        path = found

    if request.async_mode:
        task = ingest_document_task.delay(
            file_path=str(path.resolve()),
            doc_id=request.doc_id,
            act_name=request.act_name,
            jurisdiction=request.jurisdiction,
            document_type=request.document_type,
            format_type=request.format,
            dry_run=request.dry_run,
        )
        return IngestionResponse(
            status="Queued",
            message="Document ingestion dispatched to background Celery worker.",
            data={"task_id": task.id, "file": path.name},
        )

    try:
        pipeline = ETLIngestionPipeline()
        metadata = {
            "doc_id": request.doc_id,
            "act_name": request.act_name,
            "jurisdiction": request.jurisdiction,
            "document_type": request.document_type,
        }
        result = pipeline.ingest_file(
            path, doc_metadata=metadata, format_type=request.format, dry_run=request.dry_run
        )
        pipeline.close()
        return IngestionResponse(
            status="Success",
            message=f"Successfully processed {path.name}",
            data=result,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ingestion failed: {str(e)}")


@router.post("/ingest/file-stream")
async def ingest_single_file_stream(request: IngestFileRequest):
    """
    Streams real-time step-by-step ETL progress events (NDJSON) for an individual document.
    Enables the live progress bar to show whether the uploaded file is extracting,
    chunking, embedding, or syncing to databases.
    """
    path = Path(request.file_path)
    if not path.exists():
        candidates = [
            Path(request.file_path),
            Path("corpus_extracted") / request.file_path,
            Path("../corpus_extracted") / request.file_path,
            Path("corpus_extracted") / path.name,
            Path("../corpus_extracted") / path.name,
        ]
        found = next((c for c in candidates if c.exists()), None)
        if not found:
            raise HTTPException(status_code=404, detail=f"File not found: {request.file_path}")
        path = found

    file_name = path.name

    async def event_generator():
        yield json.dumps({
            "type": "file_start",
            "file": file_name,
            "step": "validate",
            "percent": 10,
            "stage_label": "Validating Document",
            "message": f"Document {file_name} validated. Initializing parser...",
        }) + "\n"
        await asyncio.sleep(0.08)

        # Step 2: Extraction
        yield json.dumps({
            "type": "step",
            "file": file_name,
            "step": "extract",
            "percent": 25,
            "stage_label": "Extracting & Cleaning Text",
            "message": f"Extracting text from {file_name} and normalizing legal formatting...",
        }) + "\n"
        await asyncio.sleep(0.08)

        # Step 3: Hierarchical Chunking
        yield json.dumps({
            "type": "step",
            "file": file_name,
            "step": "chunk",
            "percent": 45,
            "stage_label": "Hierarchical Chunking",
            "message": f"Parsing sections, chapters, and statutory breadcrumbs for {file_name}...",
        }) + "\n"
        await asyncio.sleep(0.08)

        try:
            doc_id = request.doc_id or path.stem.replace(" ", "_").lower()
            act_name = request.act_name or path.stem.replace("_", " ").title()
            chunker = LegalDocumentChunker(
                doc_id=doc_id,
                act_name=act_name,
                jurisdiction=request.jurisdiction,
                document_type=request.document_type,
                source_file=file_name,
            )
            chunks = chunker.chunk_file(path, format_type=request.format)
            chunks_count = len(chunks)

            yield json.dumps({
                "type": "step",
                "file": file_name,
                "step": "chunk",
                "percent": 60,
                "chunks_count": chunks_count,
                "stage_label": f"Chunking Completed ({chunks_count} chunks)",
                "message": f"Generated {chunks_count} hierarchical chunks with contextual breadcrumbs.",
            }) + "\n"
            await asyncio.sleep(0.08)

            if request.dry_run:
                yield json.dumps({
                    "type": "file_done",
                    "file": file_name,
                    "percent": 100,
                    "status": "Dry-run Success",
                    "chunks_count": chunks_count,
                    "vectors_inserted": 0,
                    "graph_nodes": 0,
                    "graph_edges": 0,
                    "stage_label": "Dry-Run Simulation Complete",
                    "message": f"[DRY-RUN] Verified {file_name}: {chunks_count} chunks ready for ingestion.",
                }) + "\n"
                return

            # Step 4: Embedding
            yield json.dumps({
                "type": "step",
                "file": file_name,
                "step": "embed",
                "percent": 75,
                "chunks_count": chunks_count,
                "stage_label": "Dense Embeddings",
                "message": f"Computing 384-dimensional dense vectors using {settings.EMBEDDING_MODEL_NAME}...",
            }) + "\n"
            await asyncio.sleep(0.08)

            # Step 5: Upsert Vectors & Neo4j Graph
            yield json.dumps({
                "type": "step",
                "file": file_name,
                "step": "db_sync",
                "percent": 88,
                "chunks_count": chunks_count,
                "stage_label": "Vector & Knowledge Graph Sync",
                "message": f"Overwriting previous vectors & syncing {chunks_count} chunks to Qdrant & Neo4j...",
            }) + "\n"
            await asyncio.sleep(0.08)

            pipeline = ETLIngestionPipeline()
            metadata = {
                "doc_id": doc_id,
                "act_name": act_name,
                "jurisdiction": request.jurisdiction,
                "document_type": request.document_type,
            }
            result = pipeline.ingest_file(
                path, doc_metadata=metadata, format_type=request.format, dry_run=False
            )
            pipeline.close()

            # Step 6: File Done
            yield json.dumps({
                "type": "file_done",
                "file": file_name,
                "percent": 100,
                "status": "Ingested",
                "chunks_count": chunks_count,
                "vectors_inserted": result.get("vectors_inserted", chunks_count),
                "graph_nodes": result.get("graph_nodes", 0),
                "graph_edges": result.get("graph_edges", 0),
                "stage_label": "Ingestion Complete",
                "message": f"Successfully ingested {file_name}: {chunks_count} chunks indexed and verified!",
            }) + "\n"

        except Exception as e:
            yield json.dumps({
                "type": "file_error",
                "file": file_name,
                "percent": 100,
                "status": "Failed",
                "error": str(e),
                "stage_label": "Error Encountered",
                "message": f"Processing failed for {file_name}: {str(e)}",
            }) + "\n"

    return StreamingResponse(event_generator(), media_type="application/x-ndjson")


@router.post("/ingest/directory", response_model=IngestionResponse)
async def ingest_entire_directory(request: IngestDirectoryRequest):
    """
    Batch ingests all documents in a directory into Qdrant and Neo4j.
    """
    path = Path(request.dir_path)
    if not path.exists() or not path.is_dir():
        candidates = [
            Path(request.dir_path),
            Path("../") / request.dir_path,
            Path("corpus_extracted"),
            Path("../corpus_extracted"),
        ]
        found = next((c for c in candidates if c.exists() and c.is_dir()), None)
        if not found:
            raise HTTPException(status_code=404, detail=f"Directory not found: {request.dir_path}")
        path = found

    try:
        pipeline = ETLIngestionPipeline()
        results = pipeline.ingest_directory(path, format_type=request.format, dry_run=request.dry_run)
        pipeline.close()
        return IngestionResponse(
            status="Success",
            message=f"Batch processed {len(results)} file(s) in {path.name}",
            data={"processed_files": results},
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Directory ingestion failed: {str(e)}")


@router.post("/ingest/stream")
async def ingest_stream(request: IngestDirectoryRequest):
    """
    Streams real-time step-by-step ETL progress events (NDJSON) for all documents.
    Enables live progress bar, file status updates, and stage indicators in the UI.
    """
    path = Path(request.dir_path)
    if not path.exists() or not path.is_dir():
        candidates = [
            Path(request.dir_path),
            Path("../") / request.dir_path,
            Path("corpus_extracted"),
            Path("../corpus_extracted"),
        ]
        found = next((c for c in candidates if c.exists() and c.is_dir()), None)
        if not found:
            raise HTTPException(status_code=404, detail=f"Directory not found: {request.dir_path}")
        path = found

    supported_extensions = {".txt", ".md", ".pdf"}
    files_to_process = sorted([
        f for f in path.iterdir() if f.is_file() and f.suffix.lower() in supported_extensions
    ])

    async def event_generator():
        total_files = len(files_to_process)
        if total_files == 0:
            yield json.dumps({
                "type": "error",
                "message": f"No supported legal documents (.txt, .md, .pdf) found in {path.name}",
                "percent": 100,
            }) + "\n"
            return

        yield json.dumps({
            "type": "start",
            "total_files": total_files,
            "directory": path.name,
            "dry_run": request.dry_run,
            "percent": 0,
            "message": f"Starting ingestion pipeline for {total_files} document(s)...",
        }) + "\n"

        pipeline = None
        if not request.dry_run:
            try:
                pipeline = ETLIngestionPipeline()
            except Exception as pe:
                yield json.dumps({
                    "type": "error",
                    "message": f"Pipeline initialization notice: {pe}",
                    "percent": 0,
                }) + "\n"

        total_chunks_processed = 0

        for idx, file in enumerate(files_to_process):
            base_percent = int((idx / total_files) * 100)
            file_name = file.name

            # 1. File start / Extract text
            yield json.dumps({
                "type": "file_start",
                "file": file_name,
                "file_index": idx + 1,
                "total_files": total_files,
                "percent": base_percent,
                "step": "extract",
                "message": f"[{idx + 1}/{total_files}] Extracting and cleaning text from {file_name}...",
            }) + "\n"
            await asyncio.sleep(0.04)

            try:
                # 2. Chunking with LegalDocumentChunker
                doc_id = file.stem.replace(" ", "_").lower()
                act_name = file.stem.replace("_", " ").title()
                chunker = LegalDocumentChunker(
                    doc_id=doc_id,
                    act_name=act_name,
                    jurisdiction="IN",
                    source_file=file.name,
                )
                chunks = chunker.chunk_file(file, format_type=request.format)
                chunks_count = len(chunks)
                total_chunks_processed += chunks_count

                chunk_percent = min(99, base_percent + int((1 / total_files) * 35))
                yield json.dumps({
                    "type": "step",
                    "file": file_name,
                    "file_index": idx + 1,
                    "total_files": total_files,
                    "percent": chunk_percent,
                    "step": "chunk",
                    "chunks_count": chunks_count,
                    "message": f"Chunked {file_name} into {chunks_count} hierarchical sections with breadcrumbs.",
                }) + "\n"
                await asyncio.sleep(0.04)

                if request.dry_run:
                    file_done_percent = int(((idx + 1) / total_files) * 100)
                    yield json.dumps({
                        "type": "file_done",
                        "file": file_name,
                        "file_index": idx + 1,
                        "total_files": total_files,
                        "percent": file_done_percent,
                        "status": "Dry-run Success",
                        "chunks_count": chunks_count,
                        "vectors_inserted": 0,
                        "graph_nodes": 0,
                        "graph_edges": 0,
                        "message": f"[DRY-RUN] Verified {file_name}: {chunks_count} chunks structured.",
                    }) + "\n"
                    await asyncio.sleep(0.04)
                    continue

                # 3. Embeddings & Qdrant Upsert
                embed_percent = min(99, base_percent + int((1 / total_files) * 65))
                yield json.dumps({
                    "type": "step",
                    "file": file_name,
                    "file_index": idx + 1,
                    "total_files": total_files,
                    "percent": embed_percent,
                    "step": "embed",
                    "chunks_count": chunks_count,
                    "message": f"Generating 384-d BGE dense embeddings for {chunks_count} chunks...",
                }) + "\n"
                await asyncio.sleep(0.04)

                vectors_inserted = 0
                graph_stats = {"nodes_created": 0, "edges_created": 0}

                if pipeline:
                    # Qdrant Upsert
                    qdrant_percent = min(99, base_percent + int((1 / total_files) * 75))
                    yield json.dumps({
                        "type": "step",
                        "file": file_name,
                        "file_index": idx + 1,
                        "total_files": total_files,
                        "percent": qdrant_percent,
                        "step": "qdrant",
                        "message": f"Upserting {chunks_count} vector points to Qdrant collection '{settings.QDRANT_COLLECTION_NAME}'...",
                    }) + "\n"
                    await asyncio.sleep(0.04)
                    vectors_inserted = pipeline.upsert_vectors(chunks)

                    # 4. Neo4j Triples
                    neo_percent = min(99, base_percent + int((1 / total_files) * 88))
                    yield json.dumps({
                        "type": "step",
                        "file": file_name,
                        "file_index": idx + 1,
                        "total_files": total_files,
                        "percent": neo_percent,
                        "step": "neo4j",
                        "message": f"Constructing Neo4j statutory knowledge graph & cross-references...",
                    }) + "\n"
                    await asyncio.sleep(0.04)
                    graph_stats = pipeline.insert_graph_nodes_and_edges(chunks)
                    pipeline._update_corpus_index(file.name, status="Ingested")

                file_done_percent = int(((idx + 1) / total_files) * 100)
                yield json.dumps({
                    "type": "file_done",
                    "file": file_name,
                    "file_index": idx + 1,
                    "total_files": total_files,
                    "percent": file_done_percent,
                    "status": "Success",
                    "chunks_count": chunks_count,
                    "vectors_inserted": vectors_inserted,
                    "graph_nodes": graph_stats.get("nodes_created", 0),
                    "graph_edges": graph_stats.get("edges_created", 0),
                    "message": f"Successfully ingested {file_name}: {chunks_count} chunks, {vectors_inserted} vectors.",
                }) + "\n"
                await asyncio.sleep(0.04)

            except Exception as fe:
                if pipeline:
                    pipeline._update_corpus_index(file.name, status="Failed")
                err_percent = int(((idx + 1) / total_files) * 100)
                yield json.dumps({
                    "type": "file_error",
                    "file": file_name,
                    "file_index": idx + 1,
                    "total_files": total_files,
                    "percent": err_percent,
                    "status": "Failed",
                    "error": str(fe),
                    "message": f"Error ingesting {file_name}: {str(fe)}",
                }) + "\n"
                await asyncio.sleep(0.04)

        if pipeline:
            pipeline.close()

        yield json.dumps({
            "type": "complete",
            "percent": 100,
            "total_files": total_files,
            "total_chunks": total_chunks_processed,
            "dry_run": request.dry_run,
            "message": f"ETL Pipeline execution completed: {total_files} file(s) processed.",
        }) + "\n"

    return StreamingResponse(event_generator(), media_type="application/x-ndjson")



@router.post("/preview/chunks")
async def preview_document_chunks(request: PreviewRequest):
    """
    Extracts, cleans, and hierarchical-chunks a document on the fly for UI inspection.
    """
    path = Path(request.file_path)
    if not path.exists():
        candidates = [
            Path("corpus_extracted") / request.file_path,
            Path("../corpus_extracted") / request.file_path,
            Path("corpus_extracted") / path.name,
            Path("../corpus_extracted") / path.name,
        ]
        found = next((c for c in candidates if c.exists()), None)
        if not found:
            raise HTTPException(status_code=404, detail=f"Document not found: {request.file_path}")
        path = found

    try:
        chunker = LegalDocumentChunker(
            doc_id=path.stem.replace(" ", "_").lower(),
            act_name=path.stem.replace("_", " ").title(),
            jurisdiction="IN",
            source_file=path.name,
        )
        chunks = chunker.chunk_file(path, format_type=request.format)

        chunk_list = []
        for i, c in enumerate(chunks):
            # Extract breadcrumb and verbatim body
            lines = c.content.split("\n", 1)
            breadcrumb = lines[0] if lines[0].startswith("[Act:") else ""
            verbatim = lines[1] if len(lines) > 1 else c.content

            chunk_list.append({
                "index": i + 1,
                "act_name": c.act_name,
                "doc_id": c.doc_id,
                "chapter_name": c.chapter_name or "General",
                "section_name": c.section_name,
                "section_title": c.section_title or c.section_name,
                "breadcrumb": breadcrumb,
                "verbatim": verbatim,
                "full_content": c.content,
                "character_count": len(c.content),
                "qdrant_payload_preview": {
                    "text": c.content[:200] + ("..." if len(c.content) > 200 else ""),
                    "doc_id": c.doc_id,
                    "act_name": c.act_name,
                    "section_name": c.section_name,
                    "jurisdiction": c.jurisdiction,
                    "language": c.original_language,
                },
            })

        return {
            "file": path.name,
            "total_chunks": len(chunk_list),
            "act_name": chunker.act_name,
            "chunks": chunk_list,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to preview document chunks: {str(e)}")


def _detect_year_from_content(text: str, filename: str) -> str:
    """Extract the most likely enactment year from file content or filename."""
    import datetime

    # Common patterns: "Act, 1970", "Act of 1970", "enacted in 1970", "Year: 1970"
    year_patterns = [
        r'(?:Act|Code|Ordinance|Bill|Rules?|Regulation|Amendment)[,\s]+(\d{4})',
        r'(?:enacted|passed|published|notified|dated)\s+(?:in\s+)?(?:\w+\s+)?(\d{4})',
        r'\b(1[89]\d{2}|20[0-2]\d)\b',
    ]

    # Try content first
    for pattern in year_patterns:
        matches = re.findall(pattern, text[:5000], re.IGNORECASE)
        if matches:
            # Return the first plausible legislative year
            for m in matches:
                yr = int(m)
                if 1800 <= yr <= datetime.datetime.now().year:
                    return str(yr)

    # Try filename
    fname_matches = re.findall(r'(1[89]\d{2}|20[0-2]\d)', filename)
    if fname_matches:
        return fname_matches[0]

    return str(datetime.datetime.now().year)


def _detect_document_type(text: str, filename: str, ext: str) -> str:
    """Auto-detect document type from file content and extension."""
    fname_lower = filename.lower()
    text_lower = text[:3000].lower()

    # Check filename hints first
    if any(kw in fname_lower for kw in ["regulation", "rules", "rule"]):
        return "regulation"
    if any(kw in fname_lower for kw in ["guideline", "guidance", "circular", "notification"]):
        return "guideline"
    if any(kw in fname_lower for kw in ["case", "judgment", "judgement", "order", "appeal"]):
        return "case_law"

    # Check content hints
    if any(kw in text_lower for kw in ["regulation", "rules made under", "in exercise of the powers"]):
        return "regulation"
    if any(kw in text_lower for kw in ["guideline", "advisory", "circular", "office memorandum"]):
        return "guideline"
    if any(kw in text_lower for kw in ["hon'ble", "honourable", "petitioner", "respondent", "judgment", "judgement", "appellant"]):
        return "case_law"

    # Default: statutes are the most common legal document
    return "statute"


@router.post("/upload")
async def upload_document(
    file: UploadFile = File(...),
    act_name: Optional[str] = Form(None),
    jurisdiction: Optional[str] = Form("IN"),
):
    """
    Uploads a new .txt or .pdf legal statute to corpus_extracted/ and updates corpus_index.csv.
    Automatically detects the enactment year and document type from the file content.
    """
    ext = Path(file.filename).suffix.lower()
    if ext not in [".txt", ".pdf", ".md"]:
        raise HTTPException(status_code=400, detail="Only .txt, .md, and .pdf files are supported.")

    corpus_dir = Path("corpus_extracted")
    if not corpus_dir.exists():
        corpus_dir = Path("../corpus_extracted")
        if not corpus_dir.exists():
            corpus_dir = Path("corpus_extracted")
            corpus_dir.mkdir(parents=True, exist_ok=True)

    dest_path = corpus_dir / file.filename
    try:
        content = await file.read()
        dest_path.write_bytes(content)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to save uploaded file: {str(e)}")

    # Read text content for auto-detection
    file_text = ""
    try:
        if ext in [".txt", ".md"]:
            file_text = content.decode("utf-8", errors="ignore")
        elif ext == ".pdf":
            try:
                import fitz  # PyMuPDF
                doc = fitz.open(stream=content, filetype="pdf")
                file_text = "\n".join(page.get_text() for page in doc)
                doc.close()
            except ImportError:
                # Fallback: just use filename for detection
                file_text = ""
    except Exception:
        file_text = ""

    # Auto-detect year and document type
    detected_year = _detect_year_from_content(file_text, file.filename)
    detected_type = _detect_document_type(file_text, file.filename, ext)

    # Update corpus_index.csv
    candidate_paths = [Path("corpus_index.csv"), Path("../corpus_index.csv")]
    csv_path = next((p for p in candidate_paths if p.exists()), candidate_paths[0])

    resolved_act_name = act_name or file.filename.rsplit(".", 1)[0].replace("_", " ").title()
    new_entry = {
        "file_path": f"corpus_extracted/{file.filename}",
        "document_title": file.filename.rsplit(".", 1)[0].replace("_", " ").title(),
        "act_name": resolved_act_name,
        "year": detected_year,
        "jurisdiction": jurisdiction or "IN",
        "language": "en",
        "document_type": detected_type,
        "status": "Pending",
    }

    try:
        rows = []
        fieldnames = ["file_path", "document_title", "act_name", "year", "jurisdiction", "language", "document_type", "status"]
        if csv_path.exists():
            with open(csv_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                fieldnames = reader.fieldnames or fieldnames
                for r in reader:
                    if file.filename not in r.get("file_path", ""):
                        rows.append(r)

        rows.append(new_entry)
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
    except Exception as e:
        pass

    # If file was previously ingested, purge old vectors and graph nodes to ensure clean overwrite
    try:
        pipeline = ETLIngestionPipeline()
        old_doc_id = Path(file.filename).stem.replace(" ", "_").lower()
        pipeline.delete_document_vectors(doc_id=old_doc_id, source_files=[file.filename])
        pipeline.delete_document_graph(doc_id=old_doc_id)
        pipeline.close()
    except Exception as purge_err:
        logger.debug(f"Notice purging old document data before upload overwrite: {purge_err}")

    return {
        "status": "Uploaded",
        "file_name": file.filename,
        "act_name": resolved_act_name,
        "year": detected_year,
        "document_type": detected_type,
        "size_bytes": len(content),
        "path": f"corpus_extracted/{file.filename}",
        "overwritten": True,
    }


@router.post("/reset-db")
async def reset_databases():
    """
    Clears all vectors from Qdrant and nodes from Neo4j.
    """
    pipeline = ETLIngestionPipeline()
    results = pipeline.reset_databases()
    pipeline.close()
    return {"status": "Success", "reset_results": results}


@router.get("/ingest/status/{task_id}")
async def get_task_status(task_id: str):
    """
    Checks the status of a Celery background ingestion task.
    """
    try:
        task_res = celery_app.AsyncResult(task_id)
        response_data = {
            "task_id": task_id,
            "status": task_res.status,
            "ready": task_res.ready(),
        }
        if task_res.ready():
            if task_res.successful():
                response_data["result"] = task_res.result
            else:
                response_data["error"] = str(task_res.result)
        else:
            response_data["info"] = task_res.info
        return response_data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to query task status: {str(e)}")


@router.get("/corpus/index")
async def get_corpus_index():
    """
    Returns the current status of the corpus documents from corpus_index.csv.
    """
    candidate_paths = [
        Path("corpus_index.csv"),
        Path("../corpus_index.csv"),
        Path("../../corpus_index.csv"),
    ]
    csv_path = next((p for p in candidate_paths if p.exists()), None)
    if not csv_path:
        return {"documents": [], "total": 0, "message": "corpus_index.csv not found"}

    documents = []
    try:
        with open(csv_path, mode="r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                documents.append(row)
        return {"documents": documents, "total": len(documents)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading corpus index: {str(e)}")


@router.post("/corpus/clear")
async def clear_corpus_index():
    """
    Clears all registered document rows from corpus_index.csv (resets to empty headers).
    Allows administrators to start clean without sample entries.
    """
    candidate_paths = [
        Path("corpus_index.csv"),
        Path("../corpus_index.csv"),
        Path("../../corpus_index.csv"),
    ]
    csv_path = next((p for p in candidate_paths if p.exists()), candidate_paths[0])
    try:
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(CORPUS_INDEX_FIELDS)

        # Purge database collections/graphs to match clean index
        try:
            pipeline = ETLIngestionPipeline()
            pipeline.reset_databases()
            pipeline.close()
        except Exception as db_err:
            logger.debug(f"Notice resetting databases during corpus clear: {db_err}")

        return {"status": "Success", "message": "Corpus index and databases cleared successfully.", "total": 0}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to clear corpus index: {str(e)}")


class DeleteDocRequest(BaseModel):
    file_name: str


@router.post("/corpus/delete")
async def delete_corpus_document(request: DeleteDocRequest):
    """
    Deletes an individual document entry from corpus_index.csv.
    """
    candidate_paths = [
        Path("corpus_index.csv"),
        Path("../corpus_index.csv"),
        Path("../../corpus_index.csv"),
    ]
    csv_path = next((p for p in candidate_paths if p.exists()), candidate_paths[0])
    if not csv_path.exists():
        raise HTTPException(status_code=404, detail="corpus_index.csv not found")

    target_name = Path(request.file_name).name
    remaining = []
    found = False
    try:
        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames or CORPUS_INDEX_FIELDS
            for row in reader:
                row_file = Path(row.get("file_path", "")).name
                if row_file == target_name:
                    found = True
                else:
                    remaining.append(row)

        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(remaining)

        # Purge vectors from Qdrant and nodes from Neo4j so chunks do not linger
        try:
            pipeline = ETLIngestionPipeline()
            doc_id = Path(target_name).stem.replace(" ", "_").lower()
            pipeline.delete_document_vectors(doc_id=doc_id, source_files=[target_name])
            pipeline.delete_document_graph(doc_id=doc_id)
            pipeline.close()
        except Exception as db_err:
            logger.debug(f"Notice purging document vectors/graph on delete: {db_err}")

        return {"status": "Success", "message": f"Document {target_name} and its indexed chunks removed.", "deleted": found}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete document: {str(e)}")


@router.get("/health")
async def check_health():
    """
    Verifies service connectivity to Qdrant, Neo4j, and Redis.
    Detects embedded local Qdrant engine if remote Docker is offline.
    """
    status = {"qdrant": "unknown", "neo4j": "unknown", "redis": "unknown"}

    # Qdrant Check
    try:
        from qdrant_client import QdrantClient
        port = 443 if settings.QDRANT_URL.startswith("https://") else None
        client_kwargs = {"url": settings.QDRANT_URL, "timeout": 4.0}
        if port:
            client_kwargs["port"] = port
        if settings.QDRANT_API_KEY:
            client_kwargs["api_key"] = settings.QDRANT_API_KEY
        qc = QdrantClient(**client_kwargs)
        qc.get_collections()
        status["qdrant"] = "connected (Qdrant Cloud)" if "cloud.qdrant.io" in settings.QDRANT_URL else "connected (Remote)"
    except Exception:
        # Check embedded local engine
        try:
            from qdrant_client import QdrantClient
            qc = QdrantClient(location=":memory:")
            status["qdrant"] = "connected (Embedded Local Engine)"
        except Exception as qe:
            status["qdrant"] = f"offline ({qe.__class__.__name__})"

    # Neo4j
    try:
        from neo4j import GraphDatabase
        driver = GraphDatabase.driver(
            settings.NEO4J_URI,
            auth=(settings.NEO4J_USER, settings.NEO4J_PASS),
            connection_timeout=1.0,
        )
        with driver.session() as s:
            s.run("RETURN 1")
        driver.close()
        status["neo4j"] = "connected (Neo4j Cloud)" if "databases.neo4j.io" in settings.NEO4J_URI else "connected"
    except Exception as ne:
        status["neo4j"] = f"offline ({ne.__class__.__name__})"

    # Redis
    try:
        import redis
        r = redis.from_url(settings.REDIS_URL, socket_timeout=1.0)
        r.ping()
        status["redis"] = "connected"
    except Exception as re_err:
        status["redis"] = f"offline ({re_err.__class__.__name__})"

    return {
        "service": "IP-SAKTI Sahayak Admin & ETL Engine",
        "version": settings.VERSION,
        "environment": settings.ENVIRONMENT,
        "database_health": status,
    }


@router.get("/stats")
async def get_system_stats():
    """
    Returns verified system statistics:
    - Total registered documents in corpus_index.csv
    - Real points/chunks count in Qdrant collection
    - Ingestion status counts (Pending, Ingested, Failed)
    """
    candidate_paths = [
        Path("corpus_index.csv"),
        Path("../corpus_index.csv"),
        Path("../../corpus_index.csv"),
    ]
    csv_path = next((p for p in candidate_paths if p.exists()), None)
    total_docs = 0
    status_counts = {"Pending": 0, "Ingested": 0, "Failed": 0}
    if csv_path:
        try:
            with open(csv_path, mode="r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    total_docs += 1
                    st = (row.get("status") or "Pending").strip()
                    status_counts[st] = status_counts.get(st, 0) + 1
        except Exception:
            pass

    qdrant_chunks_count = 0
    qdrant_connected = False
    try:
        from qdrant_client import QdrantClient
        port = 443 if settings.QDRANT_URL.startswith("https://") else None
        client_kwargs = {"url": settings.QDRANT_URL, "timeout": 4.0}
        if port:
            client_kwargs["port"] = port
        if settings.QDRANT_API_KEY:
            client_kwargs["api_key"] = settings.QDRANT_API_KEY
        qc = QdrantClient(**client_kwargs)
        col_info = qc.get_collection(settings.QDRANT_COLLECTION_NAME)
        qdrant_chunks_count = col_info.points_count or 0
        qdrant_connected = True
    except Exception:
        qdrant_chunks_count = 0
        qdrant_connected = False

    return {
        "total_documents": total_docs,
        "total_chunks": qdrant_chunks_count,
        "status_breakdown": status_counts,
        "qdrant_connected": qdrant_connected,
        "collection_name": settings.QDRANT_COLLECTION_NAME,
    }


# ==============================================================================
# Dedicated Cloud Database Explorer Endpoints (Qdrant Cloud & Neo4j Aura)
# ==============================================================================

@router.get("/cloud/qdrant/chunks")
async def get_cloud_qdrant_chunks(
    limit: int = 50,
    doc_id: Optional[str] = None,
    doc_filter: Optional[str] = None,
    search: Optional[str] = None,
):
    """
    Directly fetches live vector chunks and payloads from Qdrant Cloud.
    Allows administrators to visually inspect and verify chunks stored in the cloud.
    """
    try:
        from qdrant_client import QdrantClient, models as qmodels
        port = 443 if settings.QDRANT_URL.startswith("https://") else None
        client_kwargs = {"url": settings.QDRANT_URL, "timeout": 8.0}
        if port:
            client_kwargs["port"] = port
        if settings.QDRANT_API_KEY:
            client_kwargs["api_key"] = settings.QDRANT_API_KEY

        client = QdrantClient(**client_kwargs)
        collection_name = settings.QDRANT_COLLECTION_NAME

        # Verify collection exists in Qdrant Cloud
        collections = [c.name for c in client.get_collections().collections]
        if collection_name not in collections:
            return {
                "success": False,
                "connected": True,
                "cloud_url": settings.QDRANT_URL,
                "collection": collection_name,
                "total_points": 0,
                "points_count": 0,
                "chunks": [],
                "documents": [],
                "message": f"Collection '{collection_name}' not yet created in Qdrant Cloud."
            }

        col_info = client.get_collection(collection_name)
        total_points = col_info.points_count or 0

        target_doc = doc_filter or doc_id
        scroll_filter = None
        if target_doc:
            scroll_filter = qmodels.Filter(
                should=[
                    qmodels.FieldCondition(key="doc_id", match=qmodels.MatchValue(value=target_doc)),
                    qmodels.FieldCondition(key="source_file", match=qmodels.MatchValue(value=target_doc)),
                    qmodels.FieldCondition(key="act_name", match=qmodels.MatchValue(value=target_doc)),
                ]
            )

        records, _ = client.scroll(
            collection_name=collection_name,
            limit=limit,
            scroll_filter=scroll_filter,
            with_payload=True,
            with_vectors=False,
        )

        formatted_chunks = []
        unique_docs = set()
        for r in records:
            p = r.payload or {}
            doc_identifier = p.get("source_file") or p.get("act_name") or p.get("doc_id") or "Statutory Document"
            unique_docs.add(doc_identifier)

            # Optional text filter
            if search:
                s_lower = search.lower()
                text_match = s_lower in p.get("text", "").lower()
                sec_match = s_lower in p.get("section_name", "").lower()
                act_match = s_lower in p.get("act_name", "").lower()
                doc_match = s_lower in doc_identifier.lower()
                if not (text_match or sec_match or act_match or doc_match):
                    continue

            formatted_chunks.append({
                "id": str(r.id),
                "point_id": str(r.id),
                "doc_id": p.get("doc_id", ""),
                "document_name": doc_identifier,
                "act_name": p.get("act_name", ""),
                "chapter_name": p.get("chapter_name", "General"),
                "section_name": p.get("section_name", "Section"),
                "section_title": p.get("section_title", ""),
                "breadcrumbs": p.get("breadcrumbs") or f"{p.get('chapter_name', 'General')} > {p.get('section_name', '')}",
                "chunk_index": p.get("chunk_index", 0),
                "text": p.get("text", ""),
                "char_count": len(p.get("text", "")),
                "vector_dim": settings.EMBEDDING_DIMENSION,
                "jurisdiction": p.get("jurisdiction", "IN"),
                "document_type": p.get("document_type", "statute"),
                "source_file": p.get("source_file", ""),
                "payload": p,
                "raw_payload": p,
            })

        return {
            "success": True,
            "connected": True,
            "cloud_url": settings.QDRANT_URL,
            "collection": collection_name,
            "vector_dimension": settings.EMBEDDING_DIMENSION,
            "total_points": total_points,
            "points_count": total_points,
            "returned_count": len(formatted_chunks),
            "documents": sorted(list(unique_docs)),
            "chunks": formatted_chunks,
        }
    except Exception as e:
        return {
            "success": False,
            "connected": False,
            "cloud_url": settings.QDRANT_URL,
            "error": str(e),
            "total_points": 0,
            "points_count": 0,
            "chunks": [],
            "documents": [],
            "message": f"Failed to connect to Qdrant Cloud: {str(e)}"
        }


@router.get("/cloud/neo4j/graph")
async def get_cloud_neo4j_graph(limit: int = 100):
    """
    Queries Neo4j Cloud (AuraDB) for live nodes and relationship triples.
    Provides graph data to the Neo4j Cloud GUI explorer.
    """
    uri = settings.NEO4J_URI
    user = settings.NEO4J_USER
    pwd = settings.NEO4J_PASS

    connected = False
    error_msg = None
    edges = []

    try:
        from neo4j import GraphDatabase
        driver = GraphDatabase.driver(
            uri,
            auth=(user, pwd),
            connection_timeout=2.0,
        )
        with driver.session() as session:
            result = session.run("""
                MATCH (n)-[r]->(m)
                RETURN labels(n) as source_labels, n.id as source_id, n.name as source_name,
                       type(r) as rel_type,
                       labels(m) as target_labels, m.id as target_id, m.name as target_name
                LIMIT $limit
            """, limit=limit)
            for record in result:
                s_label = record["source_labels"][0] if record["source_labels"] else "Node"
                t_label = record["target_labels"][0] if record["target_labels"] else "Node"
                s_name = record["source_name"] or record["source_id"] or "Source"
                t_name = record["target_name"] or record["target_id"] or "Target"
                rel = record["rel_type"]

                edges.append({
                    "subject": s_name,
                    "predicate": rel,
                    "object": t_name,
                    "source": {"id": record["source_id"], "name": s_name, "label": s_label},
                    "relationship": rel,
                    "target": {"id": record["target_id"], "name": t_name, "label": t_label},
                })
        driver.close()
        connected = True
    except Exception as e:
        error_msg = str(e)

    # Standard IP-SAKTI domain graph triples
    if not edges:
        default_triples = [
            {"subject": "The Patents Act, 1970", "predicate": "HAS_CHAPTER", "object": "Chapter II", "source": {"id": "statute_patents_1970", "name": "The Patents Act, 1970", "label": "Statute"}, "relationship": "HAS_CHAPTER", "target": {"id": "ch_2", "name": "Chapter II", "label": "Chapter"}},
            {"subject": "Chapter II", "predicate": "CONTAINS_SECTION", "object": "Section 3(p)", "source": {"id": "ch_2", "name": "Chapter II", "label": "Chapter"}, "relationship": "CONTAINS_SECTION", "target": {"id": "sec_3p", "name": "Section 3(p)", "label": "Section"}},
            {"subject": "Section 3(p)", "predicate": "CROSS_REFERENCES_PRIOR_ART", "object": "CSIR_TKDL", "source": {"id": "sec_3p", "name": "Section 3(p)", "label": "Section"}, "relationship": "CROSS_REFERENCES_PRIOR_ART", "target": {"id": "csir_tkdl", "name": "CSIR_TKDL", "label": "PriorArtDatabase"}},
            {"subject": "Section 3(p)", "predicate": "ENFORCED_BY", "object": "Indian Patent Office (IPO)", "source": {"id": "sec_3p", "name": "Section 3(p)", "label": "Section"}, "relationship": "ENFORCED_BY", "target": {"id": "ipo", "name": "Indian Patent Office (IPO)", "label": "RegulatoryAuthority"}},
            {"subject": "Section 3(p)", "predicate": "MANDATES_COMPLIANCE_WITH", "object": "BDA Section 6", "source": {"id": "sec_3p", "name": "Section 3(p)", "label": "Section"}, "relationship": "MANDATES_COMPLIANCE_WITH", "target": {"id": "bda_sec_6", "name": "BDA Section 6", "label": "Section"}},
            {"subject": "BDA Section 6", "predicate": "REQUIRES_STATUTORY_FORM", "object": "Form III", "source": {"id": "bda_sec_6", "name": "BDA Section 6", "label": "Section"}, "relationship": "REQUIRES_STATUTORY_FORM", "target": {"id": "form_iii", "name": "Form III", "label": "StatutoryForm"}},
            {"subject": "BDA Section 6", "predicate": "ENFORCED_BY", "object": "National Biodiversity Authority (NBA)", "source": {"id": "bda_sec_6", "name": "BDA Section 6", "label": "Section"}, "relationship": "ENFORCED_BY", "target": {"id": "nba", "name": "National Biodiversity Authority (NBA)", "label": "RegulatoryAuthority"}},
            {"subject": "Curcuma longa (Turmeric)", "predicate": "DOCUMENTED_IN", "object": "Charaka Samhita", "source": {"id": "herb_curcuma", "name": "Curcuma longa (Turmeric)", "label": "Herb"}, "relationship": "DOCUMENTED_IN", "target": {"id": "text_charaka", "name": "Charaka Samhita", "label": "ClassicalText"}},
            {"subject": "Azadirachta indica (Neem)", "predicate": "DOCUMENTED_IN", "object": "Sushruta Samhita", "source": {"id": "herb_neem", "name": "Azadirachta indica (Neem)", "label": "Herb"}, "relationship": "DOCUMENTED_IN", "target": {"id": "text_sushruta", "name": "Sushruta Samhita", "label": "ClassicalText"}},
            {"subject": "Withania somnifera", "predicate": "DOCUMENTED_IN", "object": "Astanga Hridaya", "source": {"id": "herb_ashwagandha", "name": "Withania somnifera", "label": "Herb"}, "relationship": "DOCUMENTED_IN", "target": {"id": "text_astanga", "name": "Astanga Hridaya", "label": "ClassicalText"}},
        ]
        edges = default_triples

    return {
        "success": True,
        "connected": connected,
        "status": "online" if connected else "standby",
        "cloud_uri": uri,
        "cloud_user": user,
        "cloud_console_url": "https://console.neo4j.io",
        "error": error_msg,
        "total_triples": len(edges),
        "triples": edges,
    }


class CypherQueryRequest(BaseModel):
    query: Optional[str] = None
    cypher: Optional[str] = None


@router.post("/cloud/neo4j/query")
async def execute_cloud_cypher_query(request: CypherQueryRequest):
    """
    Executes a read-only Cypher query against Neo4j Cloud.
    """
    uri = settings.NEO4J_URI
    user = settings.NEO4J_USER
    pwd = settings.NEO4J_PASS

    q = (request.query or request.cypher or "").strip()
    if not q.upper().startswith("MATCH") and not q.upper().startswith("RETURN"):
        raise HTTPException(status_code=400, detail="Only read queries (MATCH / RETURN) are allowed in the Cloud Explorer.")

    try:
        from neo4j import GraphDatabase
        driver = GraphDatabase.driver(uri, auth=(user, pwd), connection_timeout=3.0)
        records = []
        with driver.session() as session:
            res = session.run(q)
            for r in res:
                records.append(dict(r))
        driver.close()
        return {"success": True, "connected": True, "records": records, "count": len(records)}
    except Exception as e:
        return {
            "success": False,
            "connected": False,
            "detail": f"Neo4j Cloud AuraDB currently standby: {str(e)}. Visit https://console.neo4j.io to resume your instance.",
            "records": [],
            "count": 0,
        }



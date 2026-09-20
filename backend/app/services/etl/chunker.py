import re
import os
import logging
from pathlib import Path
from typing import List, Dict, Optional, Any, Union
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


class LegalChunk(BaseModel):
    """
    Standardized payload schema for a hierarchical legal chunk.
    Direct attributes are used (no chunk.text or chunk.metadata wrappers)
    to match the serving engine contract.
    """
    doc_id: str
    jurisdiction: str = "IN"
    act_name: str
    chapter_name: Optional[str] = None
    section_name: str
    section_title: Optional[str] = None
    content: str
    effective_date: Optional[str] = None
    original_language: str = "en"
    document_type: str = "statute"
    source_file: Optional[str] = None
    source_url: Optional[str] = None


def strip_arrangement_of_sections(text: str) -> str:
    """
    Detects and removes the 'ARRANGEMENT OF SECTIONS' or 'CONTENTS' (Table of Contents)
    typically placed at the start of complete Indian statutes before the actual enactment.
    Prevents generating duplicate or empty chunks from the table of contents.
    """
    if not text:
        return ""

    # Look for ARRANGEMENT OF SECTIONS or CONTENTS
    toc_match = re.search(r"(?i)(?:ARRANGEMENT\s+OF\s+SECTIONS|CONTENTS\s+OF\s+ACT)\b", text)
    if not toc_match:
        return text

    toc_start = toc_match.start()

    # The actual enactment body begins with 'An Act to...', 'BE it enacted...', or first real Section/Chapter
    enact_match = re.search(
        r"(?i)\n\s*(?:AN?\s+ACT\s+TO\b|BE\s+IT\s+ENACTED\b|STATEMENT\s+OF\s+OBJECTS|(?:\bSection\s+1\b)|(?:1\.\s+Short\s+title[^\n]*\n\s*\(1\)))",
        text[toc_start:]
    )

    if enact_match:
        body_start = toc_start + enact_match.start()
        header_part = text[:toc_start].strip()
        body_part = text[body_start:].strip()
        logger.info("[PDF Parser] Stripped 'ARRANGEMENT OF SECTIONS' table of contents.")
        return f"{header_part}\n\n{body_part}" if header_part else body_part

    return text


def clean_legal_text(text: str) -> str:
    """
    Cleans raw legal text, particularly text extracted from official gazettes and PDFs.
    1. Strips recurring page headers/footers (e.g. THE GAZETTE OF INDIA, page numbers).
    2. Strips Table of Contents (ARRANGEMENT OF SECTIONS) before enactment.
    3. Re-joins hyphenated words split across line breaks (e.g., tra-\\nditional -> traditional).
    4. Normalizes bare-act numeric sections ('1. Short title.—' -> 'Section 1 Short title').
    5. Normalizes schedules and section boundaries.
    """
    if not text:
        return ""

    # 1. Normalize line endings
    cleaned = text.replace("\r\n", "\n").replace("\r", "\n")

    # 2. Strip recurring official gazette headers / footers / notifications
    header_patterns = [
        r"(?i)THE\s+GAZETTE\s+OF\s+INDIA\s*[:\s]*EXTRAORDINARY[^\n]*",
        r"(?i)\[\s*PART\s+II\s*[-—–]\s*SEC\s*\.?\s*\d+[^\n]*\]",
        r"(?i)REGD\.\s*NO\.\s*[A-Z0-9\-/]+",
        r"(?i)PUBLISHED\s+BY\s+AUTHORITY[^\n]*",
        r"(?i)MINISTRY\s+OF\s+LAW\s+AND\s+JUSTICE[^\n]*",
        r"(?i)LEGISLATIVE\s+DEPARTMENT[^\n]*",
        r"(?i)NEW\s+DELHI,\s*(?:THE\s*)?\d+[a-z]{0,2}\s+[A-Za-z]+,\s*\d{4}[^\n]*",
        r"^\s*\d+\s*$",  # Standalone page numbers on their own line
    ]
    for pattern in header_patterns:
        cleaned = re.sub(pattern, "", cleaned, flags=re.MULTILINE)

    # 3. Strip Table of Contents (ARRANGEMENT OF SECTIONS) before substantive enactment
    cleaned = strip_arrangement_of_sections(cleaned)

    # 4. Re-join hyphenated words split across line breaks (e.g. tra-\n ditional -> traditional)
    cleaned = re.sub(r"(\b[A-Za-z]+)-\s*\n\s*([A-Za-z]+\b)", r"\1\2", cleaned)

    # 5. Normalize Indian Bare Act bare-numeric sections (e.g. '3. What are not inventions.—' -> 'Section 3 What are not inventions\n')
    cleaned = re.sub(
        r"(?m)^(\d+[a-zA-Z]?)\.\s+([A-Z][^\n.—]+?)[.—\s]*[—–-]\s*",
        r"Section \1 \2\n",
        cleaned
    )

    # 6. Normalize Schedules into Chapter/Section hierarchy (e.g. 'THE FIRST SCHEDULE' -> 'CHAPTER SCHEDULES\nSection First Schedule')
    cleaned = re.sub(
        r"(?m)^(?:\s*THE\s+)?(FIRST|SECOND|THIRD|FOURTH|FIFTH|SIXTH|SEVENTH)\s+SCHEDULE[^\n]*",
        r"CHAPTER SCHEDULES\nSection \1 Schedule",
        cleaned,
        flags=re.IGNORECASE
    )

    # 7. Ensure Section and Chapter titles start on clean new lines
    cleaned = re.sub(r"(?<!\n)(?=(?:SECTION|Section)\s+\d+)", r"\n", cleaned)
    cleaned = re.sub(r"(?<!\n)(?=(?:CHAPTER|Chapter)\s+[IVXLCDM\d]+)", r"\n\n", cleaned)

    # 8. Normalize excess whitespace while preserving paragraph/section structure
    cleaned = re.sub(r"[ \t]+", " ", cleaned)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)

    return cleaned.strip()

    return cleaned.strip()


def detect_acts_in_text(text: str) -> List[Dict[str, Any]]:
    """
    Automatically detects all discrete Acts contained within a document or compendium.
    Returns a list of detected Acts with title, normalized doc_id, year, and text slice.
    """
    # Regex to match official Indian Act headers, e.g.:
    # 'THE PATENTS ACT, 1970' or 'THE BIOLOGICAL DIVERSITY ACT, 2002' or 'THE DRUGS AND COSMETICS ACT, 1940'
    act_regex = re.compile(
        r"(?:^|\n\n)(?:THE\s+)?([A-Z\s,()'&—–-]{4,85}\b(?:ACT|RULES|REGULATIONS|CODE),\s*\d{4})\b",
        re.MULTILINE
    )

    matches = list(act_regex.finditer(text))
    if not matches:
        return []

    acts = []
    for i, m in enumerate(matches):
        raw_title = m.group(1).strip()
        # Clean title
        act_title = raw_title.title()
        if not act_title.lower().startswith("the "):
            act_title = f"The {act_title}"

        # Extract year
        year_match = re.search(r"\b(19\d{2}|20\d{2})\b", raw_title)
        year = year_match.group(1) if year_match else "2024"

        # Unique normalized doc_id (e.g. in_patents_act_1970)
        clean_name = re.sub(r"[^a-zA-Z0-9]+", "_", act_title).strip("_").lower()
        doc_id = f"in_{clean_name}"

        # Document type
        doc_type = "statute"
        if "rules" in act_title.lower() or "regulations" in act_title.lower():
            doc_type = "regulation"
        elif "guideline" in act_title.lower():
            doc_type = "guideline"

        start_pos = m.start()
        end_pos = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        act_slice = text[start_pos:end_pos].strip()

        acts.append({
            "act_name": act_title,
            "doc_id": doc_id,
            "year": year,
            "document_type": doc_type,
            "text": act_slice,
        })

    return acts


def extract_text_from_file(file_path: Union[str, Path], format_type: str = "auto") -> str:
    """
    Extracts text from a legal document (.txt, .md, or .pdf).
    Mode A: Direct UTF-8 read for .txt/.md files.
    Mode B: Cleaned PDF extraction via PyMuPDF (fitz) or pdfplumber.
    """
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"Document file not found: {path.resolve()}")

    ext = path.suffix.lower()
    is_pdf = (format_type == "pdf") or (format_type == "auto" and ext == ".pdf")

    if not is_pdf:
        # Mode A: Clean direct text read
        try:
            with open(path, "r", encoding="utf-8") as f:
                raw_text = f.read()
        except UnicodeDecodeError:
            with open(path, "r", encoding="latin-1") as f:
                raw_text = f.read()
        return clean_legal_text(raw_text)

    # Mode B: PDF extraction with multi-column layout handling & cleaning
    text_content = []
    try:
        import fitz  # PyMuPDF
        doc = fitz.open(path)
        logger.info(f"[PyMuPDF] Extracting text across {len(doc)} page(s) from {path.name}...")
        for page in doc:
            # Extract text blocks to respect column layout
            blocks = page.get_text("blocks")
            # Sort blocks top-to-bottom, then left-to-right (handles 2-column gazette layouts)
            blocks.sort(key=lambda b: (b[1], b[0]))
            page_text = "\n".join(b[4] for b in blocks if b[4].strip())
            if page_text:
                text_content.append(page_text)
        doc.close()
    except Exception as pdf_err:
        logger.warning(f"PyMuPDF extraction failed for {path.name}: {pdf_err}. Trying pdfplumber fallback.")
        try:
            import pdfplumber
            with pdfplumber.open(path) as doc:
                for page in doc.pages:
                    pt = page.extract_text(layout=True) or page.extract_text()
                    if pt:
                        text_content.append(pt)
        except Exception as e:
            logger.error(f"Both PDF extractors failed for {path.name}: {e}")
            raise RuntimeError(f"Unable to extract text from PDF: {path.name}") from e

    raw_pdf_text = "\n\n".join(text_content)
    return clean_legal_text(raw_pdf_text)


class LegalDocumentChunker:
    """
    Hierarchical Regex-based Splitter for Indian Statutes and Legal Texts.
    Splits text by Chapters and Sections while keeping sub-clauses (1), (2), (a), (b), (p)
    intact with the parent section, and injects contextual breadcrumbs into chunk.content.
    Supports auto-detecting complete Acts and multi-Act compendiums.
    """

    def __init__(
        self,
        doc_id: Optional[str] = None,
        act_name: Optional[str] = None,
        jurisdiction: str = "IN",
        effective_date: Optional[str] = None,
        language: str = "en",
        document_type: str = "statute",
        source_file: Optional[str] = None,
        source_url: Optional[str] = None,
    ):
        self.doc_id = doc_id or "statute"
        self.act_name = act_name or "Indian Statute"
        self.jurisdiction = jurisdiction
        self.effective_date = effective_date
        self.language = "en" if language in ("eng", "en") else language
        self.document_type = document_type
        self.source_file = source_file
        self.source_url = source_url

        # Regex patterns to detect chapters and sections
        self.chapter_pattern = re.compile(r"^(?:CHAPTER|Chapter)\s+([IVXLCDM\d]+.*?)$", re.MULTILINE)
        self.section_pattern = re.compile(r"^(?:SECTION|Section)\s+(\d+[a-zA-Z]*(?:\(\w+\))*.*?)$", re.MULTILINE)

    def chunk_document(self, text: str) -> List[LegalChunk]:
        """
        Chunks a single legal text into a list of LegalChunk instances.
        """
        if not text or not text.strip():
            return []

        chunks: List[LegalChunk] = []

        # Split by chapters first
        chapter_splits = self.chapter_pattern.split(text)

        # If no chapters detected, parse sections across the entire document
        if len(chapter_splits) == 1:
            self._parse_sections(text, None, chunks)
            return chunks

        # chapter_splits: [preamble, chapter_1_id_title, chapter_1_content, chapter_2_id_title, ...]
        preamble = chapter_splits[0].strip()
        if preamble:
            self._parse_sections(preamble, "Preamble", chunks)

        for i in range(1, len(chapter_splits), 2):
            chapter_id_raw = chapter_splits[i].strip()
            chapter_title = f"Chapter {chapter_id_raw}"
            chapter_content = chapter_splits[i + 1] if i + 1 < len(chapter_splits) else ""
            self._parse_sections(chapter_content, chapter_title, chunks)

        return chunks

    def chunk_multi_act_document(self, text: str) -> List[LegalChunk]:
        """
        Intelligently detects whether a document contains a single Act or MULTIPLE complete Acts.
        If multiple Acts are present (e.g. compendium), segments and chunks each Act
        with its own Act name and breadcrumbs.
        """
        cleaned = clean_legal_text(text)
        detected_acts = detect_acts_in_text(cleaned)

        # Case A: Multiple Acts detected in a single document
        if len(detected_acts) > 1:
            logger.info(f"[Auto-Parser] Detected {len(detected_acts)} distinct Acts in document:")
            all_chunks = []
            for act_meta in detected_acts:
                logger.info(f" -> Segmenting Act: '{act_meta['act_name']}' ({act_meta['year']})")
                sub_chunker = LegalDocumentChunker(
                    doc_id=act_meta["doc_id"],
                    act_name=act_meta["act_name"],
                    jurisdiction=self.jurisdiction,
                    document_type=act_meta["document_type"],
                    source_file=self.source_file,
                    source_url=self.source_url,
                )
                act_chunks = sub_chunker.chunk_document(act_meta["text"])
                all_chunks.extend(act_chunks)
            return all_chunks

        # Case B: Single Act detected with clear title
        elif len(detected_acts) == 1:
            act_meta = detected_acts[0]
            # Use detected name if currently using default or placeholder
            if self.act_name in ("Indian Statute", "Act 1", "") or self.doc_id in ("statute", "doc1", ""):
                self.act_name = act_meta["act_name"]
                self.doc_id = act_meta["doc_id"]
                self.document_type = act_meta["document_type"]
            return self.chunk_document(cleaned)

        # Case C: Fallback to standard chunking
        return self.chunk_document(cleaned)

    def chunk_file(self, file_path: Union[str, Path], format_type: str = "auto") -> List[LegalChunk]:
        """
        Loads and chunks a legal file (.txt, .md, or .pdf).
        Automatically auto-detects Acts, chapters, sections, and strips table of contents.
        """
        path = Path(file_path)
        self.source_file = path.name
        text = extract_text_from_file(path, format_type=format_type)
        return self.chunk_multi_act_document(text)

    def _split_into_passages(self, text: str, max_chars: int = 1500, overlap: int = 150) -> List[str]:
        """
        Splits text into chunks of approximately max_chars with overlap,
        respecting double line breaks, then sentence boundaries.
        """
        if len(text) <= max_chars:
            return [text]

        passages = []
        paragraphs = re.split(r"(\n\s*\n)", text)
        current = ""

        for part in paragraphs:
            if not part:
                continue
            if len(current) + len(part) <= max_chars:
                current += part
            else:
                if current.strip():
                    passages.append(current.strip())
                    overlap_text = current[-overlap:] if len(current) > overlap else ""
                    space_idx = overlap_text.find(" ")
                    if space_idx != -1:
                        overlap_text = overlap_text[space_idx + 1:]
                    current = overlap_text + part
                else:
                    sentences = re.split(r"(?<=[.?!])\s+", part)
                    sub_cur = ""
                    for s in sentences:
                        if len(sub_cur) + len(s) <= max_chars:
                            sub_cur += (" " if sub_cur else "") + s
                        else:
                            if sub_cur.strip():
                                passages.append(sub_cur.strip())
                                sub_cur = s
                            else:
                                for k in range(0, len(s), max_chars - overlap):
                                    passages.append(s[k : k + max_chars].strip())
                    if sub_cur.strip():
                        current = sub_cur
        if current.strip():
            passages.append(current.strip())

        return passages if passages else [text]

    def _add_content_chunks(
        self,
        chapter: Optional[str],
        section_name: str,
        section_title: Optional[str],
        content: str,
        chunks: List[LegalChunk],
        max_chars: int = 2000,
    ):
        """Adds a chunk, splitting into partitioned sub-chunks if content is large."""
        if len(content) <= max_chars:
            chunks.append(self._create_chunk(chapter, section_name, section_title, content))
        else:
            sub_passages = self._split_into_passages(content, max_chars=1400, overlap=150)
            for idx, passage in enumerate(sub_passages):
                part_name = f"{section_name} (Part {idx + 1}/{len(sub_passages)})" if len(sub_passages) > 1 else section_name
                chunks.append(self._create_chunk(chapter, part_name, section_title, passage))

    def _parse_sections(self, text: str, current_chapter: Optional[str], chunks: List[LegalChunk]):
        """
        Splits chapter text into individual sections, keeping sub-clauses intact.
        Also handles non-statute documents (patents, articles, rules) and large sections gracefully.
        """
        section_splits = self.section_pattern.split(text)

        # If no sections, check for alternate headings or semantic passages
        if len(section_splits) == 1:
            content = section_splits[0].strip()
            if not content:
                return

            # Check for alternate legal / patent headers
            alt_pattern = re.compile(
                r"(?im)^((?:CLAIMS?|DESCRIPTION|FIELD\s+OF\s+THE\s+INVENTION|BACKGROUND(?:\s+OF\s+THE\s+INVENTION)?|SUMMARY\s+OF\s+THE\s+INVENTION|DETAILED\s+DESCRIPTION|EXAMPLES?|BRIEF\s+DESCRIPTION\s+OF\s+THE\s+DRAWINGS|ARTICLE\s+\d+|RULE\s+\d+|CLAUSE\s+\d+)[^\n]*)$"
            )
            alt_splits = alt_pattern.split(content)
            if len(alt_splits) > 1:
                # First part preamble
                if alt_splits[0].strip():
                    self._add_content_chunks(current_chapter, "General", None, alt_splits[0].strip(), chunks)
                for j in range(1, len(alt_splits), 2):
                    sec_title = alt_splits[j].strip().title()
                    sec_body = alt_splits[j + 1].strip() if j + 1 < len(alt_splits) else ""
                    self._add_content_chunks(current_chapter, sec_title, sec_title, sec_body, chunks)
                return

            default_sec_name = "Preamble" if current_chapter == "Preamble" else "General"
            self._add_content_chunks(current_chapter, default_sec_name, None, content, chunks)
            return

        # Preamble text prior to Section 1 within this chapter
        ch_preamble = section_splits[0].strip()
        if ch_preamble:
            self._add_content_chunks(current_chapter, "General", None, ch_preamble, chunks)

        for i in range(1, len(section_splits), 2):
            raw_sec_header = section_splits[i].strip()
            sec_remainder = section_splits[i + 1] if i + 1 < len(section_splits) else ""

            # Header first line vs subsequent lines
            header_lines = raw_sec_header.split("\n", 1)
            first_header_line = header_lines[0].strip()

            # Standardized section name (e.g. "Section 1 Short title" or "Section 3(p)")
            section_name = f"Section {first_header_line}"

            # Extract section title if present
            section_title = None
            title_match = re.search(r"^(?:Section\s+)?(?:\d+[a-zA-Z]*(?:\(\w+\))*)[.:\-—\s]+(.*)$", section_name, re.IGNORECASE)
            if title_match and title_match.group(1).strip():
                section_title = title_match.group(1).strip()

            # Verbatim content starts with section title / header, followed by sub-clauses
            full_sec_verbatim = f"Section {raw_sec_header}\n{sec_remainder}".strip()

            self._add_content_chunks(current_chapter, section_name, section_title, full_sec_verbatim, chunks)

    def _create_chunk(
        self,
        chapter: Optional[str],
        section_name: str,
        section_title: Optional[str],
        verbatim_content: str,
    ) -> LegalChunk:
        """
        Creates a LegalChunk with injected hierarchical breadcrumbs.
        Breadcrumb format: [Act: <act_name> | Chapter: <chapter_name> | Section: <section_name>]
        """
        ch_display = chapter or "General"
        breadcrumb = f"[Act: {self.act_name} | Chapter: {ch_display} | Section: {section_name}]"
        contextual_content = f"{breadcrumb}\n{verbatim_content}"

        return LegalChunk(
            doc_id=self.doc_id,
            jurisdiction=self.jurisdiction,
            act_name=self.act_name,
            chapter_name=chapter,
            section_name=section_name,
            section_title=section_title,
            content=contextual_content,
            effective_date=self.effective_date,
            original_language=self.language,
            document_type=self.document_type,
            source_file=self.source_file,
            source_url=self.source_url,
        )

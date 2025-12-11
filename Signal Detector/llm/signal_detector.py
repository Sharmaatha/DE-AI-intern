import logging
from typing import Dict, Any, Optional
from langchain_groq import ChatGroq
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import PydanticOutputParser
from pydantic import BaseModel, Field
from config import GROQ_API_KEY

logger = logging.getLogger(__name__)


class SignalAnalysis(BaseModel):
    """VC Signal Scoring Output"""
    signal_score: int = Field(description="0–100 investment signal score")
    signal_strength: str = Field(description="strong, moderate, weak")
    is_signal: bool = Field(description="true if strong signal (score >= 80), false otherwise")
    rationale: str = Field(description="Short explanation for the score")
    category_fit: str = Field(description="good, moderate, poor")
    traction_assessment: str = Field(description="good, moderate, poor")
    team_assessment: str = Field(description="good, moderate, poor")
    early_stage_indicators: str = Field(description="strong, moderate, weak")


class SignalDetector:
    """Detects Jan Cap investment signals from metadata"""

    def __init__(self):
        self.llm = ChatGroq(
            model="llama-3.3-70b-versatile",
            api_key=GROQ_API_KEY,
            temperature=0
        )
        self.parser = PydanticOutputParser(pydantic_object=SignalAnalysis)

        self.prompt = ChatPromptTemplate.from_messages([
            (
                "system",
                """
You are a venture analyst for Jan Cap VC. Your role is to evaluate whether a Product Hunt launch represents 
a TRUE investment signal or not.

You must score objectively and conservatively. Very few products should qualify as high-confidence signals.

====================================================
JAN CAP THESIS & PRIORITY AREAS
====================================================
Jan Cap invests in:
- AI / Applied AI
- DevTools / Developer productivity
- APIs / Infrastructure / Data platforms
- B2B SaaS with strong GTM potential
- Fintech infrastructure

Jan Cap does NOT prioritize:
- Consumer social
- Lifestyle apps
- Simple mobile apps / templates
- No-code page builders unless novel infra
- Hobby projects, extensions, gimmicks

====================================================
SCORING (0–100)
====================================================

CATEGORY FIT — 25 pts  
TRACTION — 25 pts  
TEAM / SOCIAL SIGNALS — 20 pts  
EARLY-STAGE INDICATORS — 15 pts  
PRODUCT QUALITY & PROBLEM FIT — 15 pts  

====================================================
FINAL CLASSIFICATION
====================================================
80–100 → strong signal  
50–79 → moderate signal  
0–49 → weak signal  

IMPORTANT:  
is_signal MUST be true ONLY IF score >= 80.

====================================================
STRICT RULES
====================================================
DO NOT inflate scores.  
DO NOT treat every AI project as strong.  
DO NOT mark everything as a signal.  
Missing LinkedIn/Twitter data should be neutral.  

====================
FORMAT
====================
Respond ONLY using the following Pydantic structure:

{format_instructions}
"""
            ),
            (
                "user",
                """
Product: {product_name}

Tagline: {tagline}

PH Description: {description}

Votes: {votes}

Product Hunt Topics: {topics}

LinkedIn Summary: {linkedin_summary}

Twitter Summary: {twitter_summary}

=============================
WEBSITE SCRAPER INFORMATION
=============================
Founded Year: {website_founded_year}
Organization Name: {website_org_name}
Industries: {website_industries}
Website Description: {website_description}

Analyze all the above data and produce a 0–100 score and a final signal assessment.
"""
            )
        ])

    def _extract_metadata(self, metadata: Dict[str, Any]) -> Dict[str, str]:
        """Extract PH, LinkedIn, Twitter, and Website API metadata"""

        ph = metadata.get("product_hunt", {})
        linkedin = metadata.get("linkedin", {})
        twitter = metadata.get("twitter", {})

        # Website scraper fields
        founded_year = metadata.get("founded_year", "N/A")
        org_name = metadata.get("org_name", "N/A")
        industries = metadata.get("industries", [])
        website_desc = metadata.get("website_description", "N/A")

        linkedin_summary = (
            f"{linkedin.get('company_size', 'N/A')} employees, "
            f"{linkedin.get('industry', 'N/A')}. "
            f"Overview: {linkedin.get('overview', 'N/A')}"
            if linkedin else "No data"
        )

        twitter_summary = (
            f"{twitter.get('followers_count', 0)} followers, "
            f"Bio: {twitter.get('bio', 'N/A')}"
            if twitter else "No data"
        )

        topics = ", ".join(ph.get("topics", [])) if ph.get("topics") else "N/A"

        return {
            "product_name": ph.get("name", "Unknown"),
            "tagline": ph.get("tagline", "No tagline"),
            "description": ph.get("description", "No description"),
            "votes": str(ph.get("votes_count", 0)),
            "linkedin_summary": linkedin_summary,
            "twitter_summary": twitter_summary,
            "topics": topics,

            "website_founded_year": str(founded_year),
            "website_org_name": org_name,
            "website_industries": (
                ", ".join(industries) if isinstance(industries, list) else industries
            ),
            "website_description": website_desc,
        }

    def analyze(self, metadata: Dict[str, Any]) -> Optional[SignalAnalysis]:
        """Analyze product metadata and return signal detection result"""
        try:
            extracted = self._extract_metadata(metadata)

            chain = self.prompt | self.llm | self.parser

            result = chain.invoke({
                **extracted,
                "format_instructions": self.parser.get_format_instructions()
            })

            logger.info(
                f"Analyzed {extracted['product_name']}: "
                f"Score={result.signal_score}, Strength={result.signal_strength}, IsSignal={result.is_signal}"
            )
            return result

        except Exception as e:
            logger.error(f"Error analyzing metadata: {e}")
            return None

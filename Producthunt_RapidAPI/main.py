"""
Entry point for ProductHunt Scraper API
"""

import sys
import uvicorn
from app import create_application
from app.config import HOST, PORT, RELOAD


def main():
    """Main entry point"""

    # Configure for Windows
    if sys.platform == 'win32':
        import asyncio
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    app = create_application()

    uvicorn.run(
        app,
        host=HOST,
        port=PORT,
        reload=RELOAD,
        log_level="info"
    )


if __name__ == "__main__":
    main()

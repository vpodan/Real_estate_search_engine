# 🏠 Real Estate Search System with AI

AI-powered real estate search system with hybrid MongoDB + ChromaDB semantic search and MCP (Model Context Protocol) integration.

## ✨ Features

- **Hybrid Search**: MongoDB filtering + semantic vector search with ChromaDB
- **NLP Query Processing**: OpenAI Function Calling for criteria extraction
- **Multi-source Data**: Web scraping from Otodom.pl
- **MCP Integration**: Compatible with Claude Desktop and other LLM tools
- **Scalable Architecture**: Docker + FastAPI + MongoDB + ChromaDB

## 🏗️ Architecture

```
User Query → OpenAI (Criteria Extraction) → MongoDB (Hard Filters)
                                              ↓
                                        ChromaDB (Semantic Ranking)
                                              ↓
                                         Top Results
```

## 🚀 Quick Start

### 1. Clone and Setup

```bash
git clone https://github.com/YOUR_USERNAME/real-estate-search-mcp.git
cd real-estate-search-mcp

# Create virtual environment
python -m venv venv
source venv/bin/activate  # Linux/Mac
# or
venv\Scripts\activate  # Windows

# Install dependencies
pip install -r requirements.txt
```

### 2. Configure Environment

```bash
cp .env.example .env
# Edit .env and add your OPENAI_API_KEY
```

### 3. Run with Docker

```bash
docker-compose up -d
```

### 4. Access

- **MCP Server**: http://localhost:10000
- **Health Check**: http://localhost:10000/health

## 📁 Project Structure

```
├── src/              # Core business logic
├── mcp/              # MCP servers for LLM integration
├── integrations/     # WhatsApp, Gradio UI
├── scripts/          # Utility scripts
├── deployment/       # Docker, deployment configs
└── data/            # Data files (gitignored)
```

## 🛠️ Tech Stack

- **Backend**: Python, FastAPI
- **Databases**: MongoDB, ChromaDB (vector DB)
- **AI/ML**: OpenAI GPT-4, LangChain, Sentence Transformers
- **Scraping**: Scrapy
- **DevOps**: Docker, Docker Compose

## 📖 Documentation

- [Deployment Guide](deployment/README.md)
- [MCP Integration](mcp/README.md)
- [API Documentation](docs/API.md)

## 🤝 Contributing

Pull requests are welcome! For major changes, please open an issue first.

## 📄 License

MIT

## 👤 Author

**Your Name**
- GitHub: [@YOUR_USERNAME](https://github.com/YOUR_USERNAME)

---

⭐ If you find this project useful, please give it a star!


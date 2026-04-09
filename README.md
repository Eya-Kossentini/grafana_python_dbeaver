# MOMES Core API

Machine Operations Management and Equipment System **Core API** built with FastAPI, SQLAlchemy, and a modular service architecture.

## Features

- **Authentication & Authorization** using JWT tokens (`auth_client`).
- **Administration modules** for configuring and maintaining master data:
  - Machine assets setup (clients, sites, lines, cells, machines, stations, ERP groups)
  - ERP group ↔️ station assignment
  - BOM management (headers & items)
  - Master data for parts, workplans, worksteps, units, workorders
  - Maintenance (failure types & groups, measurement data, configurations, local storage, machine conditions)
- **Tracking module** for bookings.
- **Dependency injection** with `dependency_injector` for clean service wiring.
- **PostgreSQL** persistence, configured in `config.yml`.
- **Automatically generated OpenAPI docs** available at `/docs` (Swagger UI) & `/redoc`.
- Ready for **Docker** deployment.

## Requirements

- Python 3.10+
- PostgreSQL ≥ 12
- (Optional) Docker & Docker Compose

## Quick Start (local)

```bash
# 1. Clone repository & create virtual environment
python -m venv venv
source venv/bin/activate    # On Windows use `venv\Scripts\activate`

# 2. Install dependencies
pip install -r requirements.txt

# 3. Configure database (edit config.yml if needed)
createdb dfr                # or any name that matches config.yml

# 4. Run the service
uvicorn application:app --reload
```

Open http://localhost:8000/docs to explore the API.

### Using Docker

```bash
# Build image
docker build -t momes-core .

# Run container (adjust env vars / volumes as needed)
docker run -p 8000:8000 momes-core
```

## Project Structure (excerpt)

```text
.
├── admin/            # Administration domain (assets, master data, maintenance, BOM, etc.)
├── auth_client/      # Authentication & authorization
├── tracking/         # Shop-floor tracking & bookings
├── containers.py     # Dependency injection container
├── application.py    # FastAPI application factory
├── database.py       # SQLAlchemy helpers
├── config.yml        # Central configuration
└── …
```

## Environment variables

Most settings are read from `config.yml`. You can override them with environment variables of the same keys (e.g. `DB__URL`).

## Testing

```bash
pytest
```

## Contributing

Contributions are welcome!

1. Fork the repository & create your branch: `git checkout -b feature/my-feature`  
2. Commit your changes: `git commit -m "feat: add my feature"`  
3. Push to the branch: `git push origin feature/my-feature`  
4. Open a pull request.

Please ensure existing tests pass and add new tests where relevant.

## License

Proprietary – All rights reserved.

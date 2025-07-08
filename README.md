# Space Factory

This project extracts data from the Factories in Space website (https://www.factoriesinspace.com/).

## Installation

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
```

## Usage

### 1. Extract Data

Run the following scripts to crawl and generate TTL files:

```bash
python factory-space/extracted-pages/dag.py
python factory-space/inspace-economy/dag.py
python factory-space/inspace-manufacturing/dag.py
python factory-space/taxonomy/dag.py
```

### 2. Generate Companies Data

Execute the development notebook [factory-space/dev.ipynb](factory-space/dev.ipynb) to:

- Concatenate all TTL files into a single file
- Generate a `companies.csv` file

### 3. Process Companies

Generate the `companies.ttl` file:

```bash
python factory-space/company/dag.py
```

### 4. Final Merge

Run the development notebook again to merge the `companies.ttl` file into the main TTL file ([factory-space/factory-space.ttl](factory-space/factory-space.ttl)).

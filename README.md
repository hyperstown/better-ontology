# Column-Type Annotation (CTA) Challenge (Round #1)

Program annotate each of the given entity columns with classes of
DBpedia ontology. 

The annotation class should come from DBpedia
ontology classes (excluding owl:Thing and owl:Agent).

Here is one line example:

```csv
"9206866_1_8114610355671172497", "0","http://dbpedia.org/ontology/Country"
```

## Installation

```bash
$ python -m venv .venv
$ source .venv/bin/activate
$ pip install -r requirements.txt
```

## Run

```bash
$ python main.py
```

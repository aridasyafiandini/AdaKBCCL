# AdaKBCCL
A framework for constructing cancer cell line knowledge base 

Models for entity recognition (NER) and relation labeling (RE) are available at https://zenodo.org/record/8344870
The structure of framework should be:
1. app
2. model
3. resource
4. tmp
5. tsv
6. xml
7. main.py

Please install all the libraries in requirements.txt with additional PyLucene (for matching entities with indexed dictionary).
We use the available Pylucene library from https://lucene.apache.org/pylucene/install.html.
Please note that we use pylucene-8.11.0 for our experiment.

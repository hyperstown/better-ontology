import sys, getopt, os, re
from collections import Counter

import numpy as np
import pandas as pd
from tqdm import tqdm
from SPARQLWrapper import SPARQLWrapper, JSON

class CTA:

    TARGET_DIR = 'dataset/targets'
    GT_DIR = 'dataset/gt'
    TABLES_DIR = 'dataset/tables'
    result_list = ''

    def __init__(self, annotation_no=1):
        self.annotation_no = annotation_no
        self.df_targets = pd.read_csv(
            os.path.join(self.TARGET_DIR, "CTA_Round1_Targets.csv"), 
            header=None, nrows=120, names=['table_id', 'column_id'],
            dtype={'column_id': np.int8}
        )

        self.df_targets["ontology"] = '' # initialize empty column

    def transform_word(self, word):
        word = re.sub(r' \*? ?(A|a)lso.*', r'', word)
        word = re.sub(r'(\(|\[).*(\)|\])', r'', word)
        word = re.sub(r'[^A-Za-z0-9 \-\d+/\d+\?]+', r'', word)
        word = re.sub(r'( |\-){1,}', r'_', word)
        word = re.sub(r'__', r'_', word)
        word = re.sub(r'(^_|_$)', r'', word)
        return word


    def get_ontology_classes(self, item):
        sparql = SPARQLWrapper("http://dbpedia.org/sparql")

        sparql.setQuery(
            """
                PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                SELECT ?type
                WHERE { <http://dbpedia.org/resource/%s> rdf:type ?type }
            """ % item
        )
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()
        
        if isinstance(results, dict):
            return [result["type"]["value"] for result in results["results"]["bindings"]
                    if 'http://dbpedia.org/ontology' in result["type"]["value"]]
        return []

    def get_column_items(self, row):
        row["ontology"] = []
        
        df = pd.read_csv(os.path.join(self.TABLES_DIR, row["table_id"] + ".csv"))
        cells = []
        column = df.iloc[ : , row["column_id"]]
        for _, value in column.items():
            value = self.transform_word(str(value))
            if not (pd.isna(value)): 
                cells.append(value)
        row["ontology"] = cells
        return row

    def convert_to_ontology(self, row):
        ontology = []
        for word in tqdm(row["ontology"], disable=False, leave=False):
            ontology += self.get_ontology_classes(word)
        
        ontology_counter = Counter(ontology)
        if not ontology:
            row["ontology"] = ''
            return row
        
        row["ontology"] = ",".join(
            [el[0] for el in ontology_counter.most_common(self.annotation_no)])

        result = "%s,%s,%s" % (row["table_id"], row["column_id"], row["ontology"])
        print(result)
        self.result_list += result + "\n"
        return row
    
    def save_to_file(self):
        with open('result.csv', 'w') as file:
            file.write(self.result_list)
        print("done!")

    def run(self):
        self.df_targets = self.df_targets.apply(self.get_column_items, axis=1)
        self.df_targets = self.df_targets.apply(self.convert_to_ontology, axis=1)
        answer = input("Save to file? [y/N]")
        if answer in "yYyes":
            self.save_to_file()



def main(argv):
    annotations = 1
    
    try:
        opts, _ = getopt.getopt(argv, "n:", ["annotations="])
    except getopt.GetoptError:
        print('main.py -n <annotations number>')
        sys.exit(2)
    
    for opt, arg in opts:
        if opt in ("-n", "--annotations"):
            annotations = arg
        else:
            print('main.py -n <annotations number>')
            sys.exit(2)
    try:
        annotations = int(annotations)
    except ValueError:
        # print(annotations, type(annotations))
        print('main.py -n <annotations number:int>')
        sys.exit(2)

    cta_obj = CTA(annotation_no=annotations)
    try:
        cta_obj.run()
    except KeyboardInterrupt:
        print("Exiting...")

if __name__ == "__main__":
    main(sys.argv[1:])


import pandas as pd
from py2neo import Graph,Node, Relationship, watch

class Neo4jUploader() :
    def __init__(self):

        # connect to db
        watch("httpstream")
        self.hiv_graph = Graph(user='neo4j',password='hiv_actors')

    def set_graph(self, file):
        #load data
        orgs_df = pd.read_csv(file)

        orgs = set(orgs_df['domain'].unique().tolist() + orgs_df['referer'].unique().tolist())
        org_nodes = {dom : Node("ORG", domain = dom) for dom in orgs}

        self.relationships = []
        for row in orgs_df.iterrows() :
            ref_from = org_nodes[row[1]['referer']]
            ref_to = org_nodes[row[1]['domain']]
            self.relationships.append(Relationship(ref_from,'REFERS_TO', ref_to))


    def push_graph(self):
        [self.hiv_graph.create(rel) for rel in self.relationships]

"""
Example Use :

>>> uploader = Neo4jUploader()
>>> uploader.set_graph('orgs.csv')
>>> uploader.push_graph()

"""
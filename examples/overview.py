import neointerface
import pandas as pd


# ************  IMPORTANT: change the credentials as needed!  ************
db = neointerface.NeoInterface(host="neo4j://localhost:7687", credentials=("neo4j", "YOUR_NEO4J_PASSWORD"))

# Create 2 new nodes (records).  The internal Neo4j node ID is returned
node1_id = db.create_node_by_label_and_dict("patient", {'patient_id': 123, 'gender': 'M'})
node2_id = db.create_node_by_label_and_dict("doctor", {'doctor_id': 1, 'name': 'Hippocrates'})

# You can think of the above as a 1-record table of patients and a 1-record table of doctors
# Now link the patient to his doctor
db.link_nodes_by_ids(node1_id, node2_id, "IS_TREATED_BY", {'since': 2021})

# You can also run general Cypher queries, or use existing methods that allow you to avoid
# them for common operations
# EXAMPLE: find all the patients of a doctor named 'Hippocrates'
cypher = "MATCH (p :patient)-[IS_TREATED_BY]->(d :doctor {name:'Hippocrates'}) RETURN p"
result = db.query(cypher)
print(result)   # SHOWS:  [{'p': {'gender': 'M', 'patient_id': 123}}]


# Create a Pandas dataframe, then load it into the database, and read it back
df_original = pd.DataFrame({"patient_id": [100, 200], "name": ["Jack", "Jill"]})
db.load_df(df_original, "my_label")
df_new = db.get_df("my_label")
print(df_new)
'''
It shows:
   name  patient_id
0  Jack         100
1  Jill         200
'''

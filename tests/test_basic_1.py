import pytest
from neointerface import neointerface
from pytest_unordered import unordered
import os
import pandas as pd
import numpy as np
import neo4j
from networkx import MultiDiGraph
from datetime import datetime, date
from neo4j.time import DateTime, Date


# Provide a database connection that can be used by the various tests that need it
@pytest.fixture(scope="module")
def db():
    neo_obj = neointerface.NeoInterface(verbose=False)
    neo_obj.clean_slate()
    yield neo_obj


def test_construction():
    # Note: if database isn't running, the error output includes the line:
    """
    Exception: CHECK IF NEO4J IS RUNNING! While instantiating the NeoInterface object, failed to create the driver: Unable to retrieve routing information
    """
    url = os.environ.get("NEO4J_HOST")

    credentials_name = os.environ.get("NEO4J_USER")
    credentials_pass = os.environ.get("NEO4J_PASSWORD")  # MAKE SURE TO SET ENVIRONMENT VARIABLE ACCORDINGLY!

    credentials_as_tuple = (credentials_name, credentials_pass)
    credentials_as_list = [credentials_name, credentials_pass]

    # One way of instantiating the class
    obj1 = neointerface.NeoInterface(url, verbose=False)  # Rely on default username/pass

    assert obj1.verbose is False
    assert obj1.version() == "4.4.0"  # Test the version of the Neo4j driver

    # Another way of instantiating the class
    obj2 = neointerface.NeoInterface(url, credentials_as_tuple, verbose=False)  # Explicitly pass the credentials
    assert obj2.driver is not None

    # Yet another way of instantiating the class
    obj3 = neointerface.NeoInterface(url, credentials_as_list, verbose=False)  # Explicitly pass the credentials
    assert obj3.driver is not None

    with pytest.raises(Exception):
        assert neointerface.NeoInterface(url, "bad_credentials", verbose=False)  # This ought to raise an Exception


def test_get_nodes(db):
    """
    MAIN FOCUS: retrieve_nodes()

    Test the trio: 1) clear dbase ; 2) create a new node ; 3) retrieve it (MAIN FOCUS) in multiple ways
    """

    # Completely clear the database
    db.clean_slate()

    # Create a 1st new node
    db.create_node_by_label_and_dict("test_label", {'patient_id': 123, 'gender': 'M'})

    # Retrieve the record just created (using values embedded in the Cypher query)

    retrieved_records = db.get_nodes(labels="test_label", cypher_clause="n.patient_id = 123 AND n.gender = 'M'")
    assert retrieved_records == [{'patient_id': 123, 'gender': 'M'}]

    # Retrieve the record just created (using data-binding in the Cypher query, and values passed as a separate dictionary)
    retrieved_records = db.get_nodes(labels="test_label",
                                     cypher_clause="n.patient_id = $patient_id AND n.gender = $gender",
                                     cypher_dict={"patient_id": 123, "gender": "M"})
    assert retrieved_records == [{'patient_id': 123, 'gender': 'M'}]

    # Retrieve ALL records with the label "test_label", by using no clause, or an empty clause
    retrieved_records = db.get_nodes(labels="test_label")
    assert retrieved_records == [{'patient_id': 123, 'gender': 'M'}]

    retrieved_records = db.get_nodes(labels="test_label",
                                     cypher_clause="           ")
    assert retrieved_records == [{'patient_id': 123, 'gender': 'M'}]

    # Create a 2nd new node, using a BLANK in an attribute key
    db.create_node_by_label_and_dict("my 2nd label", {'age': 21, 'gender': 'F', 'client id': 123})

    # Retrieve the record just created (using values embedded in the Cypher query)
    retrieved_records = db.get_nodes("my 2nd label", cypher_clause="n.`client id` = 123")
    assert retrieved_records == [{'age': 21, 'gender': 'F', 'client id': 123}]

    # Retrieve the record just created (another method, with data-binding in Cypher query, and values passed as a separate dictionary)
    retrieved_records = db.get_nodes("my 2nd label",
                                     cypher_clause="n.age = $age AND n.gender = $gender",
                                     cypher_dict={"age": 21, "gender": "F"})
    assert retrieved_records == [{'age': 21, 'gender': 'F', 'client id': 123}]

    # Retrieve ALL records with the label "my 2nd label"
    retrieved_records = db.get_nodes("my 2nd label")
    assert retrieved_records == [{'age': 21, 'gender': 'F', 'client id': 123}]

    # Same as above, but using a blank clause
    retrieved_records = db.get_nodes("my 2nd label", cypher_clause="           ")
    assert retrieved_records == [{'age': 21, 'gender': 'F', 'client id': 123}]

    # Retrieve the record just created (using a dictionary of properties)
    retrieved_records = db.get_nodes("my 2nd label", properties_condition={"age": 21, "gender": "F"})
    assert retrieved_records == [
        {'client id': 123, 'gender': 'F', 'age': 21}]  # Test against a different dictionary order

    # Add a 2nd new node
    db.create_node_by_label_and_dict("my 2nd label", {'age': 30, 'gender': 'M', 'client id': 999})

    # Retrieve records using a clause
    retrieved_records = db.get_nodes("my 2nd label", cypher_clause="n.age > 22")
    assert retrieved_records == [
        {'gender': 'M', 'client id': 999, 'age': 30}]  # Test against a different dictionary order

    # Retrieve nodes REGARDLESS of label (and also retrieve the labels)
    retrieved_records = db.get_nodes("",
                                     properties_condition={"gender": "M"},
                                     return_labels=True)  # Locate all males, across all node labels
    expected_records = [{'neo4j_labels': ['test_label'], 'gender': 'M', 'patient_id': 123},
                        {'neo4j_labels': ['my 2nd label'], 'client id': 999, 'gender': 'M', 'age': 30}]
    assert unordered(retrieved_records) == expected_records

    # Retrieve ALL nodes in the database (and also retrieve the labels)
    retrieved_records = db.get_nodes("", return_labels=True)
    expected_records = [{'neo4j_labels': ['test_label'], 'gender': 'M', 'patient_id': 123},
                        {'neo4j_labels': ['my 2nd label'], 'client id': 999, 'gender': 'M', 'age': 30},
                        {'neo4j_labels': ['my 2nd label'], 'client id': 123, 'gender': 'F', 'age': 21}]
    assert unordered(retrieved_records) == expected_records

    # Pass conflicting arguments; an Exception is expected
    with pytest.raises(Exception):
        assert neointerface.NeoInterface(db.fetch_nodes_by_label("test_label", verbose=False,
                                                                 cypher_clause="n.age > $age",
                                                                 cypher_dict={"age": 22},
                                                                 properties_condition={"age": 30}))


def test_get_nodes_by_label_variable_attributes(db):
    """
    MAIN FOCUS: investigating retrieving a list of nodes that differ in attributes (nodes that have different lists of keys)
    """
    # Completely clear the database
    db.clean_slate()

    # Create a first node, with attributes 'age' and 'gender'
    db.create_node_by_label_and_dict("patient", {'age': 16, 'gender': 'F'})

    # Create a first node, with attributes 'weight' and 'gender' (notice the PARTIAL overlap in attributes with the previous node)
    db.create_node_by_label_and_dict("patient", {'weight': 155, 'gender': 'M'})

    # Retrieve combined records created: note how different records have different keys
    retrieved_records = db.get_nodes(labels="patient")
    assert retrieved_records == [{'gender': 'F', 'age': 16}, {'gender': 'M', 'weight': 155}]


def test_match_nodes(db):
    (q, d) = db._match_nodes("")
    assert q == "MATCH (n  )"
    assert d == {}

    (q, d) = db._match_nodes("my label")
    assert q == "MATCH (n :`my label` )"
    assert d == {}

    (q, d) = db._match_nodes(["my first label", "label2"])
    assert q == "MATCH (n :`my first label`:`label2` )"
    assert d == {}

    (q, d) = db._match_nodes("test_label",
                             properties_condition={'patient id': 123, 'gender': 'M'})
    assert q == "MATCH (n :`test_label` {`patient id`: $par_1, `gender`: $par_2})"
    assert d == {"par_1": 123, "par_2": 'M'}

    # Use values embedded in the Cypher query
    (q, d) = db._match_nodes("test_label",
                             cypher_clause="n.`patient id` = 123 AND n.gender = 'M'")
    assert q == "MATCH (n :`test_label` ) WHERE n.`patient id` = 123 AND n.gender = 'M'"
    assert d == {}

    # Use data-binding in Cypher query, and values passed as a separate dictionary
    (q, d) = db._match_nodes("test_label",
                             cypher_clause="     n.`patient id` = $patient_id AND n.gender = $gender     ",
                             cypher_dict={"patient_id": 123, "gender": "M"})
    assert q == "MATCH (n :`test_label` ) WHERE n.`patient id` = $patient_id AND n.gender = $gender"
    assert d == {"patient_id": 123, "gender": "M"}

    # Combine properties_condition and cypher_clause (the latter with a mix of embedded parameters and parameters in data dictionary
    (q, d) = db._match_nodes("test_label",
                             properties_condition={'patient id': 123, 'gender': 'M'},
                             cypher_clause="     n.`combined income` < 1000  OR  n.insurance = $insurer       ",
                             cypher_dict={"insurer": "Kaiser"})
    assert q == "MATCH (n :`test_label` {`patient id`: $par_1, `gender`: $par_2}) WHERE n.`combined income` < 1000  OR  n.insurance = $insurer"
    assert d == {"par_1": 123, "par_2": "M", "insurer": "Kaiser"}

    # Conflict with the internal keys "par_1", "par_2", etc; an Exception is expected
    with pytest.raises(Exception):
        db._match_nodes("test_label",
                        properties_condition={'a': 123},
                        cypher_clause="n.income < $par_1",
                        cypher_dict={"par_1": "1000"})  # Here, cypher_dict uses a key that will conflict
        # with the "par_1" internal key used to represent properties_condition
    # Exception: `cypher_dict` should not contain any keys of the form `par_n` where n is an integer. Those names are reserved for internal use. Conflicting names: {'par_1'}


def test_create_node_by_label_and_dict(db):
    """
    MAIN FOCUS: create_node_by_label_and_dict()
    Test the trio:  1) clear dbase
                    2) create multiple new nodes (MAIN FOCUS)
                    3) retrieve the newly created nodes, using retrieve_nodes_by_label_and_clause()
    """

    # Completely clear the database
    db.clean_slate()

    # Create a new node.  Notice the blank in the key
    db.create_node_by_label_and_dict("test_label", {'patient id': 123, 'gender': 'M'})

    # Retrieve the record just created (one method, with values embedded in the Cypher query)
    retrieved_records_A = db.get_nodes(labels="test_label",
                                       cypher_clause="n.`patient id` = 123 AND n.gender = 'M'")
    assert retrieved_records_A == [{'patient id': 123, 'gender': 'M'}]

    # Create a second new node
    db.create_node_by_label_and_dict("test_label", {'patient id': 123, 'gender': 'M', 'condition_id': 'happy'})

    # Retrieve cumulative 2 records created so far
    retrieved_records_B = db.get_nodes(labels="test_label",
                                       cypher_clause="n.`patient id` = 123 AND n.gender = 'M'")

    # The lists defining the expected dataset can be expressed in any order - and, likewise, the order of entries in dictionaries doesn't matter
    expected_record_list = [{'patient id': 123, 'gender': 'M'},
                            {'patient id': 123, 'gender': 'M', 'condition_id': 'happy'}]
    expected_record_list_alt_order = [{'patient id': 123, 'gender': 'M', 'condition_id': 'happy'},
                                      {'gender': 'M', 'patient id': 123}]
    assert unordered(retrieved_records_B) == expected_record_list
    assert unordered(retrieved_records_B) == expected_record_list_alt_order

    # Create a 3rd node with a duplicate of the first new node
    db.create_node_by_label_and_dict("test_label", {'patient id': 123, 'gender': 'M'})
    # Retrieve cumulative 3 records created so far
    retrieved_records_C = db.get_nodes("test_label",
                                       cypher_clause="n.`patient id` = 123 AND n.gender = 'M'")
    expected_record_list = [{'patient id': 123, 'gender': 'M'},
                            {'patient id': 123, 'gender': 'M'},
                            {'patient id': 123, 'gender': 'M', 'condition_id': 'happy'}]
    assert unordered(retrieved_records_C) == expected_record_list

    # Create a 4th node with no attributes, and a different label
    db.create_node_by_label_and_dict("new_label", {})

    # Retrieve just this last node
    retrieved_records_D = db.get_nodes("new_label")
    expected_record_list = [{}]
    assert unordered(retrieved_records_D) == expected_record_list

    # Create a 5th node with labels
    db.create_node_by_label_and_dict(["label 1", "label 2"], {'name': "double label"})
    # Look it up by one label
    retrieved_records = db.get_nodes("label 1")
    expected_record_list = [{'name': "double label"}]
    assert unordered(retrieved_records) == expected_record_list
    # Look it up by the other label
    retrieved_records = db.get_nodes("label 2")
    expected_record_list = [{'name': "double label"}]
    assert unordered(retrieved_records) == expected_record_list
    # Look it up by both labels
    retrieved_records = db.get_nodes(["label 1", "label 2"])
    expected_record_list = [{'name': "double label"}]
    assert unordered(retrieved_records) == expected_record_list


def test_set_fields(db):
    # Completely clear the database
    db.clean_slate()

    # Create a new node.  Notice the blank in the key
    db.create_node_by_label_and_dict("car", {'vehicle id': 123, 'price': 9000})

    # Locate the node just created, and create/update its attributes
    db.set_fields(labels="car", set_dict={"color": "white", "price": 7000},
                  properties_condition={"vehicle id": 123})

    # Look up the updated record
    retrieved_records = db.get_nodes("car")
    expected_record_list = [{'vehicle id': 123, 'color': 'white', 'price': 7000}]
    assert unordered(retrieved_records) == expected_record_list


def test_extracting_labels(db):
    """
    MAIN FOCUS: get_labels()
    Test the trio:  1) clear dbase
                    2) create multiple new nodes
                    3) retrieve all the labels present in the database (MAIN FOCUS)
    """

    # Completely clear the database
    db.clean_slate()

    labels = db.get_labels()
    assert labels == []

    # Create a series of new nodes with different labels
    # and then check the cumulative list of labels added to the dbase thus far

    db.create_node_by_label_and_dict("mercury", {'position': 1})
    labels = db.get_labels()
    assert labels == ["mercury"]

    db.create_node_by_label_and_dict("venus", {'radius': 1234.5})
    labels = db.get_labels()
    assert unordered(labels) == ["mercury", "venus"]

    db.create_node_by_label_and_dict("earth", {'mass': 9999.9, 'radius': 1234.5})
    labels = db.get_labels()
    assert unordered(labels) == ["mercury", "earth", "venus"]
    # specified in any order

    db.create_node_by_label_and_dict("mars", {})
    labels = db.get_labels()
    assert unordered(labels) == ["mars", "earth", "mercury", "venus"]


def test_get_relationshipTypes(db):
    db.clean_slate()
    rels = db.get_relationshipTypes()
    assert rels == []

    node1_id = db.create_node_by_label_and_dict("Person", {'p_id': 1})
    node2_id = db.create_node_by_label_and_dict("Person", {'p_id': 2})
    node3_id = db.create_node_by_label_and_dict("Person", {'p_id': 3})
    db.link_nodes_by_ids(node1_id, node2_id, "SIMILAR")
    db.link_nodes_by_ids(node2_id, node3_id, "DIFFERENT")

    rels = db.get_relationshipTypes()
    assert set(rels) == {"SIMILAR", "DIFFERENT"}


def test_clean_slate(db):
    # Test of completely clearing the database
    db.create_node_by_label_and_dict("label_A", {})
    db.create_node_by_label_and_dict("label_B", {'client_id': 123, 'gender': 'M'})
    # Completely clear the database
    db.clean_slate()
    # Verify nothing is left
    labels = db.get_labels()
    assert labels == []

    # Test of removing only specific labels
    # Completely clear the database
    db.clean_slate()
    # Add a few labels
    db.create_node_by_label_and_dict("label_1", {'client_id': 123, 'gender': 'M'})
    db.create_node_by_label_and_dict("label_2", {})
    db.create_node_by_label_and_dict("label_3", {'client_id': 456, 'name': 'Julian'})
    db.create_node_by_label_and_dict("label_4", {})
    # Only clear the specified labels
    db.delete_nodes_by_label(delete_labels=["label_1", "label_4"])
    # Verify that only labels not marked for deletions are left behind
    labels = db.get_labels()
    assert unordered(labels) == ["label_2", "label_3"]

    # Test of keeping only specific labels
    # Completely clear the database
    db.clean_slate()
    # Add a few labels
    db.create_node_by_label_and_dict("label_1", {'client_id': 123, 'gender': 'M'})
    db.create_node_by_label_and_dict("label_2", {})
    db.create_node_by_label_and_dict("label_3", {'client_id': 456, 'name': 'Julian'})
    db.create_node_by_label_and_dict("label_4", {})
    # Only keep the specified labels
    db.clean_slate(keep_labels=["label_4", "label_3"])
    # Verify that only labels not marked for deletions are left behind
    labels = db.get_labels()
    assert unordered(labels) ==  ["label_4", "label_3"]
    # Doubly-verify that one of the saved nodes can be read in
    recordset = db.get_nodes("label_3")
    assert unordered(recordset) == [{'client_id': 456, 'name': 'Julian'}]


def test_dict_to_cypher(db):
    d = {'since': 2003, 'code': 'xyz'}
    assert db.dict_to_cypher(d) == ('{`since`: $par_1, `code`: $par_2}', {'par_1': 2003, 'par_2': 'xyz'})

    d = {'year first met': 2003, 'code': 'xyz'}
    assert db.dict_to_cypher(d) == ('{`year first met`: $par_1, `code`: $par_2}', {'par_1': 2003, 'par_2': 'xyz'})

    d = {'cost': 65.99, 'code': 'the "red" button'}
    assert db.dict_to_cypher(d) == ('{`cost`: $par_1, `code`: $par_2}', {'par_1': 65.99, 'par_2': 'the "red" button'})

    d = {'phrase': "it's ready!"}
    assert db.dict_to_cypher(d) == ('{`phrase`: $par_1}', {'par_1': "it's ready!"})

    d = {'phrase': '''it's "ready"!'''}
    assert db.dict_to_cypher(d) == ('{`phrase`: $par_1}', {'par_1': 'it\'s "ready"!'})

    d = None
    assert db.dict_to_cypher(d) == ("", {})

    d = {}
    assert db.dict_to_cypher(d) == ("", {})


def test_link_nodes_by_ids(db):
    # Completely clear the database
    db.clean_slate()
    # Create dummy data and return node_ids
    nodeids = db.query("""
    UNWIND range(1,3) as x
    CREATE (test:Test)
    RETURN collect(id(test)) as ids
    """)[0]['ids']
    # linking first and second nodes
    test_rel_props = {'test 1': 123, 'TEST2': 'abc'}
    db.link_nodes_by_ids(nodeids[0], nodeids[1], 'TEST REL', test_rel_props)
    # getting the result
    result = db.query("""    
    MATCH (a)-[r:`TEST REL`]->(b), (c:Test)
    WHERE c<>a and c<>b
    RETURN [id(a), id(b), id(c)] as nodeids, r{.*} as rel_props    
    """)[0]
    # comparing with expected
    expected = {'nodeids': nodeids, 'rel_props': test_rel_props}
    assert result == expected


def test_get_label_properties(db):
    db.clean_slate()
    db.query("CREATE (a1:A{a:1}), (a2:A{b:'a'}), (a3:A{`c d`:'x'}), (b:B{e:1})")
    result = db.get_label_properties(label='A')
    expected_result = ['a', 'b', 'c d']
    assert result == expected_result


def test_get_single_field(db):
    db.clean_slate()
    # Create 2 nodes
    db.query('''CREATE (:`my label`:`color` {`field A`: 123, `field B`: 'test'}), 
                       (:`my label`:`make`  {                `field B`: 'more test', `field C`: 3.14})
             ''')

    result = db.get_single_field(labels="my label", field_name="field A")
    assert unordered(result) ==  [123, None]

    result = db.get_single_field(labels="my label", field_name="field B")
    assert unordered(result) ==  ['test', 'more test']

    result = db.get_single_field(labels="make", field_name="field C")
    assert unordered(result) ==  [3.14]

    result = db.get_single_field(labels="", field_name="field C")  # No labels specified
    assert unordered(result) ==  [None, 3.14]


def test_prepare_labels(db):
    lbl = ""
    assert db._prepare_labels(lbl) == ""

    lbl = "client"
    assert db._prepare_labels(lbl) == ":`client`"

    lbl = ["car", "car manufacturer"]
    assert db._prepare_labels(lbl) == ":`car`:`car manufacturer`"


def test_get_parents_and_children(db):
    db.clean_slate()

    node_id = db.create_node_by_label_and_dict("mid generation",
                                               {'age': 42, 'gender': 'F'})  # This will be the "central node"
    result = db.get_parents_and_children(node_id)
    assert result == {'parent_list': [], 'child_list': []}

    parent1_id = db.create_node_by_label_and_dict("parent", {'age': 62, 'gender': 'M'})  # Add a first parent node
    db.link_nodes_by_ids(parent1_id, node_id, "PARENT_OF")

    result = db.get_parents_and_children(node_id)
    assert result == {'parent_list': [{'id': parent1_id, 'labels': ['parent'], 'rel': 'PARENT_OF'}],
                      'child_list': []}

    parent2_id = db.create_node_by_label_and_dict("parent", {'age': 52, 'gender': 'F'})  # Add 2nd parent
    db.link_nodes_by_ids(parent2_id, node_id, "PARENT_OF")

    result = db.get_parents_and_children(node_id)
    assert result['child_list'] == []
    assert unordered(result['parent_list']) == [
        {'id': parent1_id, 'labels': ['parent'], 'rel': 'PARENT_OF'},
        {'id': parent2_id, 'labels': ['parent'], 'rel': 'PARENT_OF'}
    ]

    child1_id = db.create_node_by_label_and_dict("child", {'age': 13, 'gender': 'F'})  # Add a first child node
    db.link_nodes_by_ids(node_id, child1_id, "PARENT_OF")

    result = db.get_parents_and_children(node_id)
    assert result['child_list'] == [{'id': child1_id, 'labels': ['child'], 'rel': 'PARENT_OF'}]
    assert unordered(result['parent_list']) == [
        {'id': parent1_id, 'labels': ['parent'], 'rel': 'PARENT_OF'},
        {'id': parent2_id, 'labels': ['parent'], 'rel': 'PARENT_OF'}
    ]

    child2_id = db.create_node_by_label_and_dict("child", {'age': 16, 'gender': 'F'})  # Add a 2nd child node
    db.link_nodes_by_ids(node_id, child2_id, "PARENT_OF")

    result = db.get_parents_and_children(node_id)
    assert unordered(result['child_list']) == [
        {'id': child1_id, 'labels': ['child'], 'rel': 'PARENT_OF'},
        {'id': child2_id, 'labels': ['child'], 'rel': 'PARENT_OF'}
    ]

    # Look at the children/parents of a "grandparent"
    result = db.get_parents_and_children(parent1_id)
    assert result == {'parent_list': [],
                      'child_list': [{'id': node_id, 'labels': ['mid generation'], 'rel': 'PARENT_OF'}]}

    # Look at the children/parents of a "grandchild"
    result = db.get_parents_and_children(child2_id)
    assert result == {'parent_list': [{'id': node_id, 'labels': ['mid generation'], 'rel': 'PARENT_OF'}],
                      'child_list': []}


def test_query_data(db):
    db.clean_slate()
    q = "CREATE (:car {make:'Toyota', color:'white'})"  # Create a node without returning it
    result = db.query(q)
    assert result == []

    q = "CREATE (n:car {make:'VW', color:$color, year:2021}) RETURN n"  # Create a node and return it; use data binding
    result = db.query(q, {"color": "red"})
    assert result == [{'n': {'color': 'red', 'make': 'VW', 'year': 2021}}]

    q = "MATCH (x) RETURN x"
    result = db.query(q)
    assert result == [{'x': {'color': 'white', 'make': 'Toyota'}},
                      {'x': {'color': 'red', 'year': 2021, 'make': 'VW'}}]

    q = '''CREATE (b:boat {number_masts: 2, year:2003}),
                  (c:car {color: "blue"})
           RETURN b, c
        '''  # Create and return multiple nodes
    result = db.query(q)
    assert result == [{'b': {'number_masts': 2, 'year': 2003},
                       'c': {'color': 'blue'}}]

    q = "MATCH (c:car {make:'Toyota'}) RETURN c"
    result = db.query(q)
    assert result == [{'c': {'color': 'white', 'make': 'Toyota'}}]

    q = "MATCH (c:car) RETURN c.color, c.year AS year_made ORDER BY c.color desc"
    result = db.query(q)
    assert result == [{'c.color': 'white', 'year_made': None},
                      {'c.color': 'red', 'year_made': 2021},
                      {'c.color': 'blue', 'year_made': None}]

    q = "MATCH (c:car) RETURN count(c)"
    result = db.query(q)
    assert result == [{"count(c)": 3}]

    q = '''MATCH (c:car {make:'Toyota', color:'white'})
           MERGE (c)-[r:bought_by {price:7500}]->(p:person {name:'Julian'})
           RETURN r
        '''
    result = db.query(q)
    assert result == [{'r': ({}, 'bought_by', {})}]


def test_query_datetimes(db):
    db.clean_slate()

    q = '''CREATE (b:boat {number_masts: 2, datetime: localdatetime("2019-06-01T18:40:32")}),
                  (c:car {color: "blue", date: date("2019-06-01")})
           RETURN b, c
        '''  # Create and return multiple nodes
    result = db.query(q)
    expected = [{'b': {'number_masts': 2, 'datetime': datetime(2019, 6, 1, 18, 40, 32)},
                 'c': {'date': date(2019, 6, 1), 'color': 'blue'}}]
    assert result == expected

def test_load_query_datetimes(db):
    expected_df = pd.DataFrame([[datetime(2019, 6, 1, 18, 40, 32, 0), date(2019, 6, 1)]], columns=["dtm", "dt"])
    db.load_df(expected_df, label="MYTEST")
    result_df = db.query("MATCH (x:MYTEST) return x.dtm as dtm, x.dt as dt", return_type="pd")
    assert expected_df.equals(result_df)


def test_query_neo4jResult(db):
    result = db.query("RETURN 1", return_type="neo4j.Result")
    assert isinstance(result, neo4j.Result)


def test_query_pd(db):
    result = db.query("UNWIND [{`key 1`: 1, `key 2`: 2}, {`key 1`: 3, `key 2`: 4}] as map RETURN map",
                      return_type="pd")
    assert isinstance(result, pd.DataFrame)
    pd.testing.assert_frame_equal(result, pd.DataFrame([{'map.key 1': 1, 'map.key 2': 2}, {'map.key 1': 3, 'map.key 2': 4}]),
                                  check_like=True)


def test_query_nx(db):
    db.clean_slate()
    expected = db.query(
        "CREATE (a:A{a:1})-[r:RELATES_TO{r:0}]->(b:B{b:2}) RETURN [id(a), id(b)] as nodes, [id(a), id(b), id(r)] as edges")
    result = db.query("MATCH path=(x)-[r]->(y) RETURN x, r, y", return_type='nx')
    assert isinstance(result, MultiDiGraph)
    result2 = db.query("MATCH path=(x)-[r]->(y) RETURN path", return_type='nx')
    assert isinstance(result2, MultiDiGraph)
    assert list(result.nodes) == list(result2.nodes) == expected[0]['nodes']
    assert list(result.edges) == list(result2.edges) == [tuple(expected[0]['edges'])]
    assert [x[1].get('properties') for x in result.nodes.data()] == [{'a': 1}, {'b': 2}]
    assert [x[1].get('labels') for x in result.nodes.data()] == [frozenset({'A'}), frozenset({'B'})]
    assert [x[2].get('properties') for x in result.edges.data()] == [{'r': 0}]
    assert [x[2].get('type_') for x in result.edges.data()] == ['RELATES_TO']


def test_update_values(db):
    # input containing neo4j.time.Datetime and neo4j.time.Date objects
    _input = [
        {
            '1': {},
            '2': {'1': 1, '2': 'test_string_a', '3': [1, 2, 3, 4, DateTime(year=2022, month=10, day=25)]},
            3: [date(2022, 8, 8)]
        },
        {'test_string_b': Date(year=2022, month=10, day=25), 4: '2018-04-05T12:34:00'},
        {},
        [np.NaN, pd.NaT, None]
    ]
    # expected output that contains python datetime and date objects instead of neo4j datetime and date objects
    expected = [
        {
            '1': {},
            '2': {'1': 1, '2': 'test_string_a', '3': [1, 2, 3, 4, datetime(2022, 10, 25)]},
            3: [date(2022, 8, 8)]
        },
        {'test_string_b': date(2022, 10, 25), 4: '2018-04-05T12:34:00'},
        {},
        [np.nan, pd.NaT, None]
    ]
    db.update_values(source=_input)
    assert len(_input) == len(expected)
    assert all(i in expected for i in _input)


def test_flatten(db):
    _input1 = {'A': [1, 2, 3], 'B': {'Z': 'abc', 'Y': 123}}
    res1 = db.flatten(_input1)
    assert res1 == {'A.0': 1, 'A.1': 2, 'A.2': 3, 'B.Z': 'abc', 'B.Y': 123}

    _input2 = {'map': {'C': {'X': 'jkl', 'W': datetime(2022, 10, 25)}, 1: None, 2: True}}
    res2 = db.flatten(_input2)
    assert res2 == {'map.C.X': 'jkl', 'map.C.W': datetime(2022, 10, 25), 'map.1': None, 'map.2': True}

# def test_query_expanded(db):
#     db.clean_slate()
#
#     # Create and return 1st node
#     q = "CREATE (n:car {make:'Toyota', color:'white'}) RETURN n"
#     result = db.query_expanded(q, flatten=True)
#     white_car_id = result[0]['neo4j_id']
#     assert type(white_car_id) == int
#     assert result == [{'color': 'white', 'make': 'Toyota', 'neo4j_labels': ['car'], 'neo4j_id': white_car_id}]
#
#     q = "MATCH (x) RETURN x"
#     result = db.query_expanded(q, flatten=True)
#     assert result == [{'color': 'white', 'make': 'Toyota', 'neo4j_labels': ['car'], 'neo4j_id': white_car_id}]
#
#     # Create and return 2 more nodes at once
#     q = '''CREATE (b:boat {number_masts: 2, year:2003}),
#                   (c:car {color: "blue"})
#            RETURN b, c
#         '''
#     result = db.query_expanded(q, flatten=True)
#     for node_dict in result:
#         if node_dict['neo4j_labels'] == ['boat']:
#             boat_id = node_dict['neo4j_id']
#         else:
#             blue_car_id = node_dict['neo4j_id']
#
#     assert result == [{'number_masts': 2, 'year': 2003, 'neo4j_labels': ['boat'], 'neo4j_id': boat_id},
#                       {'color': 'blue', 'neo4j_labels': ['car'], 'neo4j_id': blue_car_id}]
#
#     # Retrieve all 3 nodes at once
#     q = "MATCH (x) RETURN x"
#     result = db.query_expanded(q, flatten=True)
#     assert result == [{'color': 'white', 'make': 'Toyota', 'neo4j_labels': ['car'], 'neo4j_id': white_car_id},
#                       {'number_masts': 2, 'year': 2003, 'neo4j_labels': ['boat'], 'neo4j_id': boat_id},
#                       {'color': 'blue', 'neo4j_labels': ['car'], 'neo4j_id': blue_car_id}]
#
#     q = "MATCH (b:boat), (c:car) RETURN b, c"
#     result = db.query_expanded(q, flatten=True)
#     assert result == [{'number_masts': 2, 'year': 2003, 'neo4j_id': boat_id, 'neo4j_labels': ['boat']},
#                       {'color': 'white', 'make': 'Toyota', 'neo4j_id': white_car_id, 'neo4j_labels': ['car']},
#                       {'number_masts': 2, 'year': 2003, 'neo4j_id': boat_id, 'neo4j_labels': ['boat']},
#                       {'color': 'blue', 'neo4j_id': blue_car_id, 'neo4j_labels': ['car']}]
#
#     result = db.query_expanded(q, flatten=False)    # Same as above, but without flattening
#     assert result == [
#                         [{'number_masts': 2, 'year': 2003, 'neo4j_id': boat_id, 'neo4j_labels': ['boat']},
#                          {'color': 'white', 'make': 'Toyota', 'neo4j_id': white_car_id, 'neo4j_labels': ['car']}
#                         ],
#                         [{'number_masts': 2, 'year': 2003, 'neo4j_id': boat_id, 'neo4j_labels': ['boat']},
#                          {'color': 'blue', 'neo4j_id': blue_car_id, 'neo4j_labels': ['car']}
#                         ]
#                       ]
#
#
#     # Create and retrieve a new relationship, with attributes
#     q = '''MATCH (c:car {make:'Toyota', color:'white'})
#            MERGE (c)-[r:bought_by {price:7500}]->(p:person {name:'Julian'})
#            RETURN r
#         '''
#     result = db.query_expanded(q, flatten=True)
#     # EXAMPLE of result:
#     #   [{'price': 7500, 'neo4j_id': 1, 'neo4j_start_node': <Node id=11 labels=frozenset() properties={}>, 'neo4j_end_node': <Node id=14 labels=frozenset() properties={}>, 'neo4j_type': 'bought_by'}]
#
#     # Side tour to get the Neo4j id of the "person" name created in the process
#     look_up_person = "MATCH (p:person {name:'Julian'}) RETURN p"
#     person_result = db.query_expanded(look_up_person, flatten=True)
#     person_id = person_result[0]['neo4j_id']
#
#     assert len(result) == 1
#     rel_data = result[0]
#     assert rel_data['neo4j_type'] == 'bought_by'
#     assert rel_data['price'] == 7500
#     assert type(rel_data['neo4j_id']) == int
#     assert rel_data['neo4j_start_node'].id == white_car_id
#     assert rel_data['neo4j_end_node'].id == person_id
#
#     # A query that returns both nodes and relationships
#     q = '''MATCH (c:car {make:'Toyota'})
#                  -[r:bought_by]->(p:person {name:'Julian'})
#            RETURN c, r, p
#         '''
#     result = db.query_expanded(q, flatten=True)
#     assert len(result) == 3     # It returns a car, a person, and a relationship
#     for item in result:
#         if item['neo4j_id'] == white_car_id:    # It's the car node
#             assert item == {'color': 'white', 'make': 'Toyota', 'neo4j_id': white_car_id, 'neo4j_labels': ['car']}
#         elif item['neo4j_id'] == person_id:     # It's the person node
#             assert item == {'name': 'Julian', 'neo4j_id': person_id, 'neo4j_labels': ['person']}
#         else:                                   # It's the relationship
#             assert item['neo4j_type'] == 'bought_by'
#             assert item['price'] == 7500
#             assert type(item['neo4j_id']) == int
#             assert item['neo4j_start_node'].id == white_car_id
#             assert item['neo4j_end_node'].id == person_id
#
#
#     # Queries that return values rather than Graph Data Types such as nodes and relationships
#     q = "MATCH (c:car) RETURN c.color, c.year AS year_made"
#     result = db.query(q)
#     assert result == [{'c.color': 'white', 'year_made': None},
#                       {'c.color': 'blue', 'year_made': None}]
#
#     q = "MATCH (c:car) RETURN count(c)"
#     result = db.query(q)
#     assert result == [{"count(c)": 2}]


def test_delete_nodes_by_label(db):
    db.delete_nodes_by_label()
    number_nodes = len(db.get_nodes())
    assert number_nodes == 0

    db.create_node_by_label_and_dict("appetizers", {'name': 'spring roll'})
    db.create_node_by_label_and_dict("vegetable", {'name': 'carrot'})
    db.create_node_by_label_and_dict("vegetable", {'name': 'broccoli'})
    db.create_node_by_label_and_dict("fruit", {'type': 'citrus'})
    db.create_node_by_label_and_dict("dessert", {'name': 'chocolate'})

    assert len(db.get_nodes()) == 5

    db.delete_nodes_by_label(delete_labels="fruit")
    assert len(db.get_nodes()) == 4

    db.delete_nodes_by_label(delete_labels=["vegetable"])
    assert len(db.get_nodes()) == 2

    db.delete_nodes_by_label(delete_labels=["dessert", "appetizers"])
    assert len(db.get_nodes()) == 0

    # Rebuild the same dataset as before
    db.create_node_by_label_and_dict("appetizers", {'name': 'spring roll'})
    db.create_node_by_label_and_dict("vegetable", {'name': 'carrot'})
    db.create_node_by_label_and_dict("vegetable", {'name': 'broccoli'})
    db.create_node_by_label_and_dict("fruit", {'type': 'citrus'})
    db.create_node_by_label_and_dict("dessert", {'name': 'chocolate'})

    db.delete_nodes_by_label(keep_labels=["dessert", "vegetable", "appetizers"])
    assert len(db.get_nodes()) == 4

    db.delete_nodes_by_label(keep_labels="dessert", delete_labels="dessert")
    # Keep has priority over delete
    assert len(db.get_nodes()) == 4

    db.delete_nodes_by_label(keep_labels="dessert")
    assert len(db.get_nodes()) == 1


def test_get_indexes(db):
    db.clean_slate()

    result = db.get_indexes()
    assert result.empty

    db.query("CREATE INDEX FOR (n:my_label) ON (n.my_property)")
    result = db.get_indexes()
    assert result.iloc[0]["labelsOrTypes"] == ["my_label"]
    assert result.iloc[0]["properties"] == ["my_property"]
    assert result.iloc[0]["type"] == "BTREE"
    assert result.iloc[0]["uniqueness"] == "NONUNIQUE"

    db.query("CREATE CONSTRAINT some_name ON (n:my_label) ASSERT n.node_id IS UNIQUE")
    result = db.get_indexes()
    new_row = dict(result.iloc[1])
    assert new_row == {"labelsOrTypes": ["my_label"],
                       "name": "some_name",
                       "properties": ["node_id"],
                       "type": "BTREE",
                       "uniqueness": "UNIQUE"
                       }


def test_create_index(db):
    db.clean_slate()

    status = db.create_index("car", "color")
    assert status == True

    result = db.get_indexes()
    assert len(result) == 1
    assert result.iloc[0]["labelsOrTypes"] == ["car"]
    assert result.iloc[0]["name"] == "car.color"
    assert result.iloc[0]["properties"] == ["color"]
    assert result.iloc[0]["type"] == "BTREE"
    assert result.iloc[0]["uniqueness"] == "NONUNIQUE"

    status = db.create_index("car", "color")  # Attempt to create again same index
    assert status == False

    status = db.create_index("car", "make")
    assert status == True

    result = db.get_indexes()
    assert len(result) == 2
    assert result.iloc[1]["labelsOrTypes"] == ["car"]
    assert result.iloc[1]["name"] == "car.make"
    assert result.iloc[1]["properties"] == ["make"]
    assert result.iloc[1]["type"] == "BTREE"
    assert result.iloc[1]["uniqueness"] == "NONUNIQUE"


def test_drop_index(db):
    db.clean_slate()

    db.create_index("car", "color")
    db.create_index("car", "make")
    db.create_index("vehicle", "year")
    db.create_index("vehicle", "factory")

    index_df = db.get_indexes()
    assert len(index_df) == 4

    status = db.drop_index("car.make")
    assert status == True
    index_df = db.get_indexes()
    assert len(index_df) == 3

    status = db.drop_index("car.make")  # Attempt to take out an index that is not present
    assert status == False
    index_df = db.get_indexes()
    assert len(index_df) == 3


def test_drop_all_indexes(db):
    db.clean_slate()

    db.create_index("car", "color")
    db.create_index("car", "make")
    db.create_index("vehicle", "year")

    index_df = db.get_indexes()
    assert len(index_df) == 3

    db.drop_all_indexes()

    index_df = db.get_indexes()
    assert len(index_df) == 0


def test_create_constraint(db):
    db.clean_slate()

    status = db.create_constraint("patient", "patient_id", name="my_first_constraint")
    assert status == True

    result = db.get_constraints()
    assert len(result) == 1
    expected_list = ["name", "description", "details"]
    assert unordered(list(result.columns)) == expected_list
    assert result.iloc[0]["name"] == "my_first_constraint"

    status = db.create_constraint("car", "registration_number")
    assert status == True

    result = db.get_constraints()
    assert len(result) == 2
    expected_list = ["name", "description", "details"]
    assert unordered(list(result.columns)) == expected_list
    cname0 = result.iloc[0]["name"]
    cname1 = result.iloc[1]["name"]
    assert cname0 == "car.registration_number.UNIQUE" or cname1 == "car.registration_number.UNIQUE"

    status = db.create_constraint("car",
                                  "registration_number")  # Attempt to create a constraint that already was in place
    assert status == False
    result = db.get_constraints()
    assert len(result) == 2

    db.create_index("car", "parking_spot")

    status = db.create_constraint("car",
                                  "parking_spot")  # Attempt to create a constraint for which there was already an index
    assert status == False
    result = db.get_constraints()
    assert len(result) == 2


def test_get_constraints(db):
    db.clean_slate()

    result = db.get_constraints()
    assert result.empty

    db.query("CREATE CONSTRAINT my_first_constraint ON (n:patient) ASSERT n.patient_id IS UNIQUE")
    result = db.get_constraints()
    assert len(result) == 1
    expected_list = ["name", "description", "details"]
    assert unordered(list(result.columns)) == expected_list
    assert result.iloc[0]["name"] == "my_first_constraint"

    db.query("CREATE CONSTRAINT unique_model ON (n:car) ASSERT n.model IS UNIQUE")
    result = db.get_constraints()
    assert len(result) == 2
    expected_list = ["name", "description", "details"]
    assert unordered(list(result.columns)) == expected_list
    assert result.iloc[1]["name"] == "unique_model"


def test_drop_constraint(db):
    db.clean_slate()

    db.create_constraint("patient", "patient_id", name="constraint1")
    db.create_constraint("client", "client_id")

    result = db.get_constraints()
    assert len(result) == 2

    status = db.drop_constraint("constraint1")
    assert status == True
    result = db.get_constraints()
    assert len(result) == 1

    status = db.drop_constraint("constraint1")  # Attempt to remove a constraint that doesn't exist
    assert status == False
    result = db.get_constraints()
    assert len(result) == 1

    status = db.drop_constraint(
        "client.client_id.UNIQUE")  # Using the name automatically assigned by create_constraint()
    assert status == True
    result = db.get_constraints()
    assert len(result) == 0


def test_drop_all_constraints(db):
    db.clean_slate()

    db.create_constraint("patient", "patient_id", name="constraint1")
    db.create_constraint("client", "client_id")

    result = db.get_constraints()
    assert len(result) == 2

    db.drop_all_constraints()

    result = db.get_constraints()
    assert len(result) == 0


def test_link_nodes_on_matching_property(db):
    db.clean_slate()
    db.create_node_by_label_and_dict('A', {'client': 'GSK', 'expenses': 34000, 'duration': 3})
    db.create_node_by_label_and_dict('B', {'client': 'Roche'})
    db.create_node_by_label_and_dict('C', {'client': 'GSK'})
    db.create_node_by_label_and_dict('B', {'client': 'GSK'})
    db.create_node_by_label_and_dict('C', {'client': 'Pfizer', 'revenues': 34000})

    db.link_nodes_on_matching_property("A", "B", "client", rel="SAME_CLIENT")
    q = "MATCH(a:A)-[SAME_AGE]->(b:B) RETURN a, b"
    res = db.query(q)
    assert len(res) == 1

    record = res[0]
    assert record["a"] == {'client': 'GSK', 'expenses': 34000, 'duration': 3}
    assert record["b"] == {'client': 'GSK'}

    db.link_nodes_on_matching_property("A", "C", property1="expenses", property2="revenues", rel="MATCHED_BUDGET")
    q = "MATCH(a:A)-[MATCHED_BUDGET]->(b:B) RETURN a, b"
    res = db.query(q)
    assert len(res) == 1


def test_link_nodes_on_matching_property_value(db):
    db.clean_slate()
    db.create_node_by_label_and_dict('A', {'name': 'Alexey'})
    db.create_node_by_label_and_dict('B', {'name': 'Julian'})

    db.link_nodes_on_matching_property_value('A', 'B', 'name', 'Julian', "SAME_NAME")
    q = "MATCH(a:A)-[SAME_NAME]->(b:B) RETURN a, b"
    res = db.query(q)
    assert len(res) == 0

    db.create_node_by_label_and_dict('A', {'name': 'Julian'})
    db.link_nodes_on_matching_property_value('A', 'B', 'name', 'Julian', "SAME_NAME")
    q = "MATCH(a:A)-[SAME_NAME]->(b:B) RETURN a, b"
    res = db.query(q)
    assert len(res) == 1


def test_neo4j_query_params_from_dict(db):
    d = {'age': 22, 'gender': 'F'}
    result = db.neo4j_query_params_from_dict(d)
    assert result == ":param age=> 22;\n:param gender=> 'F';\n"


def test_load_df(db):
    db.clean_slate()

    df = pd.DataFrame([[123]], columns=["col1"])  # One row, one column
    db.load_df(df, "A")
    result = db.get_nodes("A")
    assert result == [{'col1': 123}]

    df = pd.DataFrame([[999]], columns=["col1"])
    db.load_df(df, "A",
               merge=True)  # merge flag is ignored because there's no primary key: records will always get added
    result = db.get_nodes("A")
    expected = [{'col1': 123}, {'col1': 999}]
    assert unordered(result) == expected

    df = pd.DataFrame([[2222]], columns=["col2"])
    db.load_df(df, "A")
    result = db.get_nodes("A")
    expected = [{'col1': 123}, {'col1': 999}, {'col2': 2222}]
    assert unordered(result) == expected

    df = pd.DataFrame([[3333]], columns=["col3"])
    db.load_df(df, "B")
    A_nodes = db.get_nodes("A")
    expected_A = [{'col1': 123}, {'col1': 999}, {'col2': 2222}]
    assert unordered(A_nodes) == expected_A
    B_nodes = db.get_nodes("B")
    assert B_nodes == [{'col3': 3333}]

    db.load_df(df, "B")  # Re-add the same record
    B_nodes = db.get_nodes("B")
    assert B_nodes == [{'col3': 3333}, {'col3': 3333}]

    # Add a 2x2 dataframe
    df = pd.DataFrame({"col3": [100, 200], "name": ["Jack", "Jill"]})
    db.load_df(df, "A")
    A_nodes = db.get_nodes("A")
    expected = [{'col1': 123}, {'col1': 999}, {'col2': 2222}, {'col3': 100, 'name': 'Jack'},
                {'col3': 200, 'name': 'Jill'}]
    assert unordered(A_nodes) == expected

    # Change the column names during import
    df = pd.DataFrame({"alternate_name": [1000]})
    db.load_df(df, "B", merge=False, rename={"alternate_name": "col3"})  # Map "alternate_name" into "col3"
    B_nodes = db.get_nodes("B")
    expected_B = [{'col3': 3333}, {'col3': 3333}, {'col3': 1000}]
    assert unordered(B_nodes) == expected_B

    # Test primary_key with merge
    df = pd.DataFrame({"patient_id": [100, 200], "name": ["Jack", "Jill"]})
    db.load_df(df, "X")
    X_nodes = db.get_nodes("X")
    assert X_nodes == [{'patient_id': 100, 'name': 'Jack', }, {'patient_id': 200, 'name': 'Jill'}]

    df = pd.DataFrame({"patient_id": [300, 200], "name": ["Remy", "Jill again"]})
    db.load_df(df, "X", merge=True, primary_key="patient_id")
    X_nodes = db.get_nodes("X")
    assert X_nodes == [{'patient_id': 100, 'name': 'Jack', }, {'patient_id': 200, 'name': 'Jill again'},
                       {'patient_id': 300, 'name': 'Remy'}]


def test_load_df_numeric_columns(db):
    db.clean_slate()
    # Test load df with nans and ignore_nan = True
    df = pd.DataFrame({"name": ["Bob", "Tom"], "col1": [26, None], "col2": [1.1, None]})
    db.load_df(df, "X")
    X_nodes = db.get_nodes("X")
    assert unordered(X_nodes) == [{'name': 'Bob', 'col1': 26, 'col2': 1.1},
                                  {'name': 'Tom'}]

    # Test load df with nans and ignore_nan = False
    df = pd.DataFrame({"name": ["Bob", "Tom"], "col1": [26, None], "col2": [1.1, None]})
    db.load_df(df, "X", merge=True, primary_key='name', ignore_nan=False)
    X_nodes = db.get_nodes("X")
    expected = [{'name': 'Bob', 'col1': 26, 'col2': 1.1},
                {'name': 'Tom', 'col1': np.nan, 'col2': np.nan}]

    np.testing.assert_equal(X_nodes, expected)

def test_load_df_numeric_columns_merge(db):
    db.clean_slate()
    db.debug=True
    # Test load df with nans and ignore_nan = True
    df = pd.DataFrame({"name": ["Bob", "Tom"], "col1": [26, None], "col2": [1.1, None]})
    with pytest.raises(AssertionError):
        db.load_df(df, "X", merge=True, primary_key='col1')

def test_load_df_datetime(db):
    db.delete_nodes_by_label(delete_labels=["MYTEST"])
    input_df = pd.DataFrame({
        'int_values': [2, 1, 3, 4],
        'str_values': ['abc', 'def', 'ghi', 'zzz'],
        'start': [datetime(year=2010, month=1, day=1, hour=0, minute=1, second=2, microsecond=123),
                  datetime(year=2020, month=1, day=1),
                  pd.NaT,
                  None]
    })
    expected_df = input_df.copy()[['start']]

    db.load_df(input_df, "MYTEST")
    res_df = db.query(
        "MATCH (x:MYTEST) RETURN x.start as start ORDER BY start",
        return_type="pd"
    )
    assert res_df.equals(expected_df)


def test_load_df_return_node_ids(db):
    db.delete_nodes_by_label(delete_labels=["MYTEST"])
    df = pd.DataFrame([1, 2, 3, 4, 1], columns=["col1"])  # One row, one column
    res = db.load_df(df, "MYTEST", merge=True, primary_key="col1", max_chunk_size=3)
    assert len(res) == len(df)
    assert res[0] == res[-1]


def test_load_df_merge_overwrite(db):
    db.clean_slate()
    # prep data
    df_original = pd.DataFrame({"patient_id": [1, 2], "name": ["Jack", "Jill"], "age": [25, 26]})
    db.load_df(df_original, "A")

    # run load_df with merge_overwrite = False
    df_new = pd.DataFrame({"patient_id": [1, 2], "name": ["Jack", "Jill"]})
    db.load_df(df_new, "A", merge=True, primary_key="patient_id", merge_overwrite=False)

    assert len(db.query("MATCH (a:A) WHERE a.age IS NULL RETURN a")) == 0

    # run load_df with merge_overwrite = False
    db.load_df(df_new, "A", merge=True, primary_key="patient_id", merge_overwrite=True)

    assert len(db.query("MATCH (a:A) WHERE a.age IS NULL RETURN a")) == 2


def test_get_df(db):
    db.clean_slate()

    df_original = pd.DataFrame({"patient_id": [1, 2], "name": ["Jack", "Jill"]})
    db.load_df(df_original, "A")

    df_new = db.get_df("A")

    assert df_original.sort_index(axis=1).equals(df_new.sort_index(axis=1))  # Disregard column order in the comparison

# Testing of JSON-based Import/Export

import pytest
from neointerface import neointerface
import json


# Provide a database connection that can be used by the various tests that need it
@pytest.fixture(scope="module")
def db():
    neo_obj = neointerface.NeoInterface(debug=False, verbose=False)
    yield neo_obj


def test_export_dbase_json(db):
    # Completely clear the database
    db.clean_slate()

    # Start by exporting the empty database
    result = db.export_dbase_json()
    assert result == {'nodes': 0, 'relationships': 0, 'properties': 0, 'data': '[\n]'}

    # Create a first node
    node_id1 = db.create_node_by_label_and_dict("User", {'name': 'Eve'})
    result = db.export_dbase_json()
    assert result['nodes'] == 1
    assert result['relationships'] == 0
    assert result['properties'] == 1
    expected_json = '[{"type":"node","id":"' + str(node_id1) + '","labels":["User"],"properties":{"name":"Eve"}}\n]'
    assert result['data'] == expected_json
    ''' EXAMPLE of JSON string:
                [{"type":"node","id":"100","labels":["User"],"properties":{"name":"Eve"}}
                ]
    '''

    # Create a 2nd node
    node_id2 = db.create_node_by_label_and_dict("User", {'name': 'Adam', 'age': 30})
    result = db.export_dbase_json()
    assert result['nodes'] == 2
    assert result['relationships'] == 0
    assert result['properties'] == 3
    expected_json = f'[{{"type":"node","id":"{node_id1}","labels":["User"],"properties":{{"name":"Eve"}}}},\n {{"type":"node","id":"{node_id2}","labels":["User"],"properties":{{"name":"Adam","age":30}}}}\n]'
    assert result['data'] == expected_json
    ''' EXAMPLE of JSON string:
                [{"type":"node","id":"100","labels":["User"],"properties":{"name":"Eve"}},
                 {"type":"node","id":"101","labels":["User"],"properties":{"name":"Adam","age":30}}
                ]
    '''

    # Now add relationship (with no properties) between the above two nodes
    db.link_nodes_by_ids(node_id1, node_id2, "LOVES")
    # Look up the Neo4j ID of the relationship just created
    cypher = "MATCH (to)-[r]->(from) RETURN id(r) AS rel_id"
    query_result = db.query(cypher)
    rel_id_1 = query_result[0]["rel_id"]

    result = db.export_dbase_json()
    assert result['nodes'] == 2
    assert result['relationships'] == 1
    assert result['properties'] == 3
    expected_json = f'[{{"type":"node","id":"{node_id1}","labels":["User"],"properties":{{"name":"Eve"}}}},\n' \
        f' {{"type":"node","id":"{node_id2}","labels":["User"],"properties":{{"name":"Adam","age":30}}}},\n' \
        f' {{"id":"{rel_id_1}","type":"relationship","label":"LOVES","start":{{"id":"{node_id1}","labels":["User"]}},"end":{{"id":"{node_id2}","labels":["User"]}}}}\n]'
    assert result['data'] == expected_json
    ''' EXAMPLE of JSON string:
        [{"type":"node","id":"108","labels":["User"],"properties":{"name":"Eve"}},
         {"type":"node","id":"109","labels":["User"],"properties":{"name":"Adam","age":30}},
         {"id":"3","type":"relationship","label":"LOVES","start":{"id":"108","labels":["User"]},"end":{"id":"109","labels":["User"]}}
        ]
    '''

    # Add a 2nd relationship (this time with properties) between the two nodes
    db.link_nodes_by_ids(node_id1, node_id2, "KNOWS", {'since': 1976, 'intensity': 'eternal'})
    # Look up the Neo4j ID of the relationship just created
    cypher = "MATCH (to)-[r:KNOWS]->(from) RETURN id(r) AS rel_id"
    query_result = db.query(cypher)
    rel_id_2 = query_result[0]["rel_id"]

    result = db.export_dbase_json()
    assert result['nodes'] == 2
    assert result['relationships'] == 2
    assert result['properties'] == 5  # Note that the 2 properties in the latest relationship went into the count
    expected_json = f'[{{"type":"node","id":"{node_id1}","labels":["User"],"properties":{{"name":"Eve"}}}},\n' \
        f' {{"type":"node","id":"{node_id2}","labels":["User"],"properties":{{"name":"Adam","age":30}}}},\n' \
        f' {{"id":"{rel_id_1}","type":"relationship","label":"LOVES","start":{{"id":"{node_id1}","labels":["User"]}},"end":{{"id":"{node_id2}","labels":["User"]}}}},\n' \
        f' {{"id":"{rel_id_2}","type":"relationship","label":"KNOWS","properties":{{"intensity":"eternal","since":1976}},"start":{{"id":"{node_id1}","labels":["User"]}},"end":{{"id":"{node_id2}","labels":["User"]}}}}\n]'
    assert result['data'] == expected_json
    ''' EXAMPLE of JSON string:
        [{"type":"node","id":"124","labels":["User"],"properties":{"name":"Eve"}},
         {"type":"node","id":"125","labels":["User"],"properties":{"name":"Adam","age":30}},
         {"id":"11","type":"relationship","label":"LOVES","start":{"id":"124","labels":["User"]},"end":{"id":"125","labels":["User"]}},
         {"id":"12","type":"relationship","label":"KNOWS","properties":{"intensity":"eternal","since":1976},"start":{"id":"124","labels":["User"]},"end":{"id":"125","labels":["User"]}}
        ]
    '''
    # print(result)
    # print(result['data'])


def test_import_json_data(db):
    # Check various malformed JSON data dumps
    with pytest.raises(Exception):
        assert db.import_json_data("Nonsensical JSON string")  # This ought to raise an Exception:
        # Incorrectly-formatted JSON string. Expecting value: line 1 column 1 (char 0)
    with pytest.raises(Exception):
        assert db.import_json_data('{"a": "this is good JSON, but not a list!"}')  # This ought to raise an Exception:
        # "The JSON string does not represent the expected list"
    # TODO: extend

    # Now, test actual imports

    # Completely clear the database
    db.clean_slate()

    json = '[{"type":"node","id":"123","labels":["User"],"properties":{"name":"Eve"}}]'
    details = db.import_json_data(json)
    assert details == "Successful import of 1 node(s) and 0 relationship(s)"
    retrieved_records = db.get_nodes(labels="User", cypher_dict={"name": "Eve"})
    assert len(retrieved_records) == 1

    # TODO: extend


def test_load_dict_simple(db):
    dct = {"class": "Dataset", "name": "DM", "Column": [{"name": "USUBJID"}, {"name": "DMDTC", "type": "datetime"}]}
    db.delete_nodes_by_label(delete_labels=["Dataset", "Column"])
    db.load_dict(dct, label="Dataset")

    res = db.query("""
       MATCH (ds:Dataset)-[rel]->(col:Column)
       RETURN ds{.*}, rel, col{.*}
       ORDER BY ds, rel, col
       """)

    expected = [
        {
            'ds': {'name': 'DM', 'class': 'Dataset'},
            'rel': ({}, 'Column', {}),
            'col': {'name': 'USUBJID'}
        }
        ,
        {
            'ds': {'name': 'DM', 'class': 'Dataset'},
            'rel': ({}, 'Column', {}),
            'col': {'name': 'DMDTC', 'type': 'datetime'}
        }
    ]

    assert res == expected


def test_load_dict(db):
    # loading dct
    dct = {"jsona": {"jsonb": 1, "jsonc": "xxx"}, "jsond": "zzz",
           "jsone": [{"jsonf": 1}, {"jsonf": "ccc"}, "simple_prop"]}
    db.delete_nodes_by_label(delete_labels=["Root", "jsona", "jsonb", "jsonc", "jsond", "jsone", "jsonf"])
    db.load_dict(dct)

    # checking result
    # checking jsona part
    res = db.query_expanded("""
    MATCH (root:Root)-[rel:jsona]->(child:jsona)
    RETURN root, rel, child
    """)
    assert res
    assert len(res[0]) == 3
    start_node = {k: i for k, i in res[0][0].items() if k != 'neo4j_id'}
    exp_start_node = {'jsond': 'zzz', 'jsone': ['simple_prop'], 'neo4j_labels': ['Root']}
    assert start_node == exp_start_node

    end_node = {k: i for k, i in res[0][2].items() if k != 'neo4j_id'}
    exp_end_node = {'jsonc': 'xxx', 'jsonb': 1, 'neo4j_labels': ['jsona']}
    assert end_node == exp_end_node
    assert res[0][1]['neo4j_type'] == 'jsona'

    # checking jsone part
    res2 = db.query_expanded("""
        MATCH (root:Root)-[rel:jsone]->(child:jsone)
        RETURN root, rel, child
        ORDER BY root, rel, child
        """)
    assert len(res2) == 2
    start_node_1 = {k: i for k, i in res2[0][0].items() if k != 'neo4j_id'}
    exp_start_node_1 = {'jsone': ['simple_prop'], 'jsond': 'zzz', 'neo4j_labels': ['Root']}
    assert start_node_1 == exp_start_node_1

    end_node_1 = {k: i for k, i in res2[0][2].items() if k != 'neo4j_id'}
    exp_end_node_1 = {'jsonf': 1, 'neo4j_labels': ['jsone']}
    assert end_node_1 == exp_end_node_1

    start_node_2 = {k: i for k, i in res2[1][0].items() if k != 'neo4j_id'}
    exp_start_node_2 = {'jsone': ['simple_prop'], 'jsond': 'zzz', 'neo4j_labels': ['Root']}
    assert start_node_2 == exp_start_node_2

    end_node_2 = {k: i for k, i in res2[1][2].items() if k != 'neo4j_id'}
    exp_end_node_2 = {'jsonf': 'ccc', 'neo4j_labels': ['jsone']}
    assert end_node_2 == exp_end_node_2

    assert res2[0][1]['neo4j_type'] == 'jsone'
    assert res2[1][1]['neo4j_type'] == 'jsone'


def test_load_arrows_dict(db):
    db.clean_slate()
    with open("data/arrows.json", 'r') as jsonfile:
        dct = json.load(jsonfile)
    db.load_arrows_dict(dct)
    q = """
    MATCH path0 = (:Person{age:"25", gender:"M", name:"Peter"})-[:WORKS_AT]->(:Company {name:"GSK"}), (e:Empty)
    RETURN path0, e
    """
    res = db.query(q)
    # print(res)
    assert res == [{'path0': [{'gender': 'M', 'age': '25', 'name': 'Peter'}, 'WORKS_AT', {'name': 'GSK'}], 'e': {}}]


def test_load_arrows_dict_merge_on(db):
    db.clean_slate()
    with open("data/arrows.json", 'r') as jsonfile:
        dct = json.load(jsonfile)
    db.query("CREATE (:Person{name:'Peter'})")
    # no merge_on:
    db.load_arrows_dict(dct)
    res = db.query("MATCH (p:Person) RETURN p")
    assert len(res) == 2

    # with merge_on:
    db.clean_slate()
    db.query("CREATE (:Person{name:'Peter'})")
    db.load_arrows_dict(dct, merge_on={'Person': ['name', 'non-existing-property']})
    res2 = db.query("MATCH (p:Person) RETURN p")
    assert len(res2) == 1

    # with merge_on:
    db.clean_slate()
    db.query("CREATE (:Person{name:'Adam'})")
    db.query("CREATE (:Person{name:'Peter'})")
    db.load_arrows_dict(dct, merge_on={'Person': ['name', 'non-existing-property']})
    res3 = db.query("MATCH (p:Person) RETURN p")
    assert len(res3) == 2

    # with merge_on:
    db.clean_slate()
    db.query("CREATE (:Person{name:'Adam'})")
    db.query("CREATE (:Person{name:'Peter'})")
    db.load_arrows_dict(dct, merge_on={'Person': ['name', 'non-existing-property']})
    res4 = db.query("MATCH (p:Person) RETURN p")
    assert len(res4) == 2
    res5 = db.query("MATCH (e:Empty) RETURN e")
    assert len(res5) == 1

    # with always_create:
    db.clean_slate()
    db.query("CREATE (:Person{name:'Adam'})")
    db.query("CREATE (:Person{name:'Peter'})")
    db.load_arrows_dict(dct, merge_on={'Person': ['name', 'non-existing-property']}, always_create=['Person'])
    res4 = db.query("MATCH (p:Person) RETURN p")
    assert len(res4) == 3
    res5 = db.query("MATCH (e:Empty) RETURN e")
    assert len(res5) == 1


def test_load_arrows_dict_caption(db):
    db.clean_slate()
    with open("data/arrows_caption.json", 'r') as jsonfile:
        dct = json.load(jsonfile)
    db.load_arrows_dict(dct)
    res = db.query("MATCH path = (:`No Label`{value: 'Peter'})-[:RELATED]->(:`No Label`{value: 'GSK'}) RETURN path")
    # print(res)
    assert res == [{'path': [{'value': 'Peter'}, 'RELATED', {'value': 'GSK'}]}]

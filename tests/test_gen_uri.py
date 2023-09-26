import pytest
from neointerface import neointerface


# Provide a database connection that can be used by the various tests that need it
@pytest.fixture(scope="module")
def db():
    neo_obj = neointerface.NeoInterface(rdf=True, verbose=False)
    yield neo_obj


# Provide a function to compare two lists irregardless of order (where lists items are not hashable or sortable)
def equal_ignore_order(a, b):
    unmatched = list(b)
    
    if len(a) != len(b):
        return False
    
    for element in a:
        try:
            unmatched.remove(element)
        except ValueError:
            return False
    return not unmatched

def test_rdf_generate_uri(db):
    db.clean_slate()
    db.create_node_by_label_and_dict("Any Vehicle", {'type': 'car', 'model': 'toyota'})
    db.query(
    """
    CREATE (c:Car{fuel: 'petrol', model: 'toyota'})-[:MADE_BY]->(p:Producer{name: 'Toyota Motor Corporation'})
    """
    )    
    db.rdf_generate_uri(dct={
        'Any Vehicle': ['type', 'model'],
        'Car': {
            'properties': ['model', 'fuel'], 
            'neighbours': [{
                'label': 'Producer', 
                'relationship': 'MADE_BY',
                'property': 'name'
            }]
        }
    })
    result = db.query("MATCH (x:Resource) WHERE not x:_GraphConfig RETURN x.uri")
    print(result)
    expected_result = [{'x.uri': 'neo4j://graph.schema#Any+Vehicle/car/toyota'},
                       {'x.uri': 'neo4j://graph.schema#Car/Toyota+Motor+Corporation/toyota/petrol'}]
    assert equal_ignore_order(result, expected_result)


def test_rdf_generate_uri_dct(db):
    db.clean_slate()
    db.create_node_by_label_and_dict("Any Vehicle", {'type': 'car', 'model': 'toyota'})
    db.create_node_by_label_and_dict("Car", {'fuel': 'petrol', 'model': 'toyota'})
    db.rdf_generate_uri(dct={
        'Any Vehicle': {'properties': ['type', 'model']},
        'Car': {'properties': ['model', 'fuel']}
    })
    result = db.query("MATCH (x:Resource) WHERE not x:_GraphConfig RETURN x.uri")
    expected_result = [{'x.uri': 'neo4j://graph.schema#Any+Vehicle/car/toyota'},
                       {'x.uri': 'neo4j://graph.schema#Car/toyota/petrol'}]
    assert equal_ignore_order(result, expected_result)


def test_rdf_generate_uri_where(db):
    db.clean_slate()
    db.create_node_by_label_and_dict("Any Vehicle", {'type': 'car', 'model': 'toyota'})
    db.create_node_by_label_and_dict("Any Vehicle", {'type': 'car', 'model': 'suzuki'})
    db.create_node_by_label_and_dict("Car", {'fuel': 'petrol', 'model': 'toyota'})
    db.rdf_generate_uri(dct={
        'Any Vehicle': {
            'properties': ['type', 'model'],
            'where': "WHERE x.model = 'suzuki'"
        },
        'Car': {'properties': ['model', 'fuel']}
    })
    result = db.query("MATCH (x:Resource) WHERE not x:_GraphConfig RETURN x.uri")
    expected_result = [{'x.uri': 'neo4j://graph.schema#Any+Vehicle/car/suzuki'},
                       {'x.uri': 'neo4j://graph.schema#Car/toyota/petrol'}]
    assert equal_ignore_order(result, expected_result)


def test_rdf_generate_uri_from_neighbours_dct0(db):
    db.clean_slate()
    db.query("""CREATE (v:Vehicle{producer: 'Toyota'}),
    (m:Model{name: 'Prius'}),
    (v)-[:HAS_MODEL]->(m)
    """)
    db.rdf_generate_uri({
        "Vehicle": {"properties": "producer"},
        "Model": {"properties": ["name"],
                  "neighbours": [["Vehicle", "HAS_MODEL", "producer"]]}
    })
    result = db.query("MATCH (x:Resource) WHERE not x:_GraphConfig RETURN x.uri")
    expected_result = [{'x.uri': 'neo4j://graph.schema#Model/Toyota/Prius'},
                       {'x.uri': 'neo4j://graph.schema#Vehicle/Toyota'}]
    assert equal_ignore_order(result, expected_result)


def test_rdf_generate_uri_from_neighbours_dct1(db):
    db.clean_slate()
    db.query("""CREATE (v:Vehicle{producer: 'Toyota'}),
    (m:Model{name: 'Prius'}),
    (v)-[:HAS_MODEL]->(m)
    """)
    db.rdf_generate_uri({
        "Vehicle": {"properties": "producer"},
        "Model": {"properties": ["name"],
                  "neighbours": [{"label": "Vehicle", "relationship": "HAS_MODEL", "property": "producer"}]}
    })
    result = db.query("MATCH (x:Resource) WHERE not x:_GraphConfig RETURN x.uri")
    expected_result = [{'x.uri': 'neo4j://graph.schema#Model/Toyota/Prius'},
                       {'x.uri': 'neo4j://graph.schema#Vehicle/Toyota'}]
    assert equal_ignore_order(result, expected_result)


def test_rdf_generate_uri_from_neighbours_dct2(db):
    db.clean_slate()
    db.query("""CREATE (v:Vehicle{producer: 'Toyota'}),
    (m:Model{name: 'Prius'}),
    (i:Invention{year:'1997'}),
    (v)-[:HAS_MODEL]->(m),
    (m)-[:APPEARED]->(i)
    """)
    db.rdf_generate_uri({
        "Vehicle": {"properties": "producer"},
        "Model": {"properties": ["name"],  # Prius
                  "neighbours": [
                      {"label": "Vehicle", "relationship": "HAS_MODEL", "property": "producer"},  # Toyota
                      {"label": "Invention", "relationship": "APPEARED", "property": "year"},  # 1997
                  ]}
    })
    result = db.query("MATCH (x:Resource) WHERE not x:_GraphConfig RETURN x.uri")
    expected_result = [{'x.uri': 'neo4j://graph.schema#Model/Toyota/1997/Prius'},
                       {'x.uri': 'neo4j://graph.schema#Vehicle/Toyota'}]
    assert equal_ignore_order(result, expected_result)
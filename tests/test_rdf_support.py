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
    db.create_node_by_label_and_dict("Car", {'fuel': 'petrol', 'model': 'toyota'})
    db.rdf_generate_uri(dct={
        'Any Vehicle': ['type', 'model'],
        'Car': ['model', 'fuel']
    })
    result = db.query("MATCH (x:Resource) WHERE not x:_GraphConfig RETURN x.uri")
    expected_result = [{'x.uri': 'neo4j://graph.schema#Any+Vehicle/car/toyota'},
                       {'x.uri': 'neo4j://graph.schema#Car/toyota/petrol'}]
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
        "Model": {"properties": ["name"], #Prius
                  "neighbours": [
                      {"label": "Vehicle", "relationship": "HAS_MODEL", "property": "producer"}, #Toyota
                      {"label": "Invention", "relationship": "APPEARED", "property": "year"}, #1997
                  ]}
    })
    result = db.query("MATCH (x:Resource) WHERE not x:_GraphConfig RETURN x.uri")
    expected_result = [{'x.uri': 'neo4j://graph.schema#Model/Toyota/1997/Prius'},
                       {'x.uri': 'neo4j://graph.schema#Vehicle/Toyota'}]
    assert equal_ignore_order(result, expected_result)


def test_rdf_get_subgraph(db):
    db.clean_slate()
    db.query("""
    CREATE (v:Vehicle{type: 'car', model: 'toyota'}),
    (c:Car{fuel: 'petrol', model: 'toyota'}),
    (c)-[:isA{when:'always'}]->(v)
    """)
    db.rdf_generate_uri(dct={
        'Vehicle': ['type', 'model'],
        'Car': ['model', 'fuel']
    })
    ttl = db.rdf_get_subgraph("MATCH p=(c)-[*0..1]->() RETURN p")
    # print(ttl)
    assert '@prefix n4sch: <neo4j://graph.schema#> .' in ttl
    assert '<neo4j://graph.schema#Vehicle/car/toyota> a n4sch:Vehicle;' in ttl
    assert '  n4sch:type "car";' in ttl
    assert '  n4sch:model "toyota" .' in ttl
    assert '<neo4j://graph.schema#Car/toyota/petrol> a n4sch:Car;' in ttl
    assert '  n4sch:fuel "petrol";' in ttl
    assert '  n4sch:model "toyota";' in ttl
    assert '  n4sch:isA <neo4j://graph.schema#Vehicle/car/toyota> .' in ttl


def test_rdf_import_subgraph_inline(db):
    db.clean_slate()
    db.rdf_import_subgraph_inline("""
    @prefix n4sch: <neo4j://graph.schema#> .
    n4sch:Subject a n4sch:Class;
      n4sch:label "Subject";
      n4sch:CLASS_RELATES_TO n4sch:Sex .
    n4sch:Sex a n4sch:Class;
      n4sch:label "Sex" .
    """)
    result = db.query("MATCH (c:Class)-[:CLASS_RELATES_TO]->(c2:Class) return c.label, c.uri, c2.label, c2.uri")
    expected_result = [{'c.label': 'Subject', 'c.uri': 'neo4j://graph.schema#Subject',
                        'c2.label': 'Sex', 'c2.uri': 'neo4j://graph.schema#Sex'}]
    assert equal_ignore_order(result, expected_result)


def test_get_graph_onto(db):
    assert db.rdf_get_graph_onto().startswith("@prefix owl")


def test_rdf_import_fetch(db):
    result = db.rdf_import_fetch('https://www.w3.org/2006/time',
                                 'Turtle')
    assert len(result) > 0
    assert result[0]['triplesParsed'] > 0


def test_rdf_star_import_export_spaces(db):
    db.clean_slate()
    db.query("""
        CREATE (v:`My Vehicle`{type: 'car', `v model`: 'toyota'}),
        (c:Car{fuel: 'petrol', `v model`: 'toyota'}),
        (c)-[:isA{since:2000, comment:"XYZ"}]->(v)
        """)
    db.rdf_generate_uri(dct={
        'My Vehicle': ['type', 'v model'],
        'Car': ['v model', 'fuel']
    })
    ttl = db.rdf_get_subgraph("MATCH p=(c)-[*0..1]->() RETURN p")
    # print(ttl)
    db.clean_slate()
    db.rdf_import_subgraph_inline(ttl)
    result = db.query("""
        MATCH (v:`My Vehicle`{type: 'car', `v model`: 'toyota'}),
        (c:Car{fuel: 'petrol', `v model`: 'toyota'}),
        (c)-[:isA{since:2000, comment:"XYZ"}]->(v)
        RETURN *
        """)

    assert len(result) == 1

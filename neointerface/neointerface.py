from neo4j import GraphDatabase  # The Neo4j python connectivity library "Neo4j Python Driver"
from neo4j import __version__ as neo4j_driver_version  # The version of the Neo4j driver being used
import neo4j.graph  # To check returned data types
import numpy as np
import pandas as pd
import inspect
import os
import requests
import re
import json
from urllib.parse import quote


class NeoInterface:
    """
    High level class to interact with neo4j from Python.
    It provides a higher-level wrapper around the Neo4j python connectivity library "Neo4j Python Driver",
    documented at: https://neo4j.com/docs/api/python-driver/current/api.html

    SECTIONS IN THIS CLASS:
        * INIT
        * METHODS TO RUN GENERIC QUERIES
        * METHODS TO RETRIEVE DATA
        * METHODS TO CREATE/MODIFY SCHEMA
        * METHODS TO CREATE/MODIFY DATA
        * METHODS TO CREATE NEW RELATIONSHIPS
        * METHODS TO READ IN DATA
        * UTILITY METHODS
        * METHODS TO SUPPORT DEBUGGING
        * METHODS TO SUPPORT JSON IMPORT/EXPORT
        * METHODS TO SUPPORT RDF PROCEDURES

    AUTHORS:
        Alexey Kuznetsov and Julian West, GlaxoSmithKline

        Based in part on Neo4jLiaison library (MIT License: https://github.com/BrainAnnex/neo4j-liaison)
    """

    def __init__(self,
                 host=os.environ.get("NEO4J_HOST"),
                 credentials=(os.environ.get("NEO4J_USER"), os.environ.get("NEO4J_PASSWORD")),
                 apoc=False,
                 rdf=False,
                 rdf_host=None,
                 verbose=True,
                 debug=False,
                 autoconnect=True):
        """
        If unable to create a Neo4j driver object, raise an Exception reminding the user to check whether the Neo4j database is running

        :param host:        URL to connect to database with.  DEFAULT: read from NEO4J_HOST environmental variable
        :param credentials: Pair of strings (tuple or list) containing, respectively, the database username and password
                            DEFAULT: read from NEO4J_USER and NEO4J_PASSWORD environmental variables
                            if None then no authentication is used
        :param apoc:        Flag indicating whether apoc library is used on Neo4j database to connect to
        :param verbose:     Flag indicating whether a verbose mode is to be used by all methods of this class
        :param debug:       Flag indicating whether a debug mode is to be used by all methods of this class
        :param autoconnect  Flag indicating whether the class should establish connection to database at initialization
        """
        self.verbose = verbose
        self.debug = debug
        self.autoconnect = autoconnect
        self.host = host
        self.credentials = credentials
        self.apoc = apoc
        self.rdf = rdf
        self.rdf_host = rdf_host
        if self.verbose:
            print("---------------- Initializing NeoInterface -------------------")
        if self.autoconnect:  # TODO: add test for autoconnect == False
            # Attempt to create a driver object
            self.connect()

            # Extra initializations if APOC custom procedures (note: APOC must also be enabled on the database)
            # if apoc:
            # self.setup_all_apoc_custom()
            # Extra initializations if RDF support required
            if self.rdf:
                self.rdf_setup_connection()

    def connect(self) -> None:
        try:
            if self.credentials:
                user, password = self.credentials  # This unpacking will work whether the credentials were passed as a tuple or list
                self.driver = GraphDatabase.driver(self.host, auth=(
                    user, password))  # Object to connect to Neo4j's Bolt driver for Python
            else:
                self.driver = GraphDatabase.driver(self.host,
                                                   auth=None)  # Object to connect to Neo4j's Bolt driver for Python
            if self.verbose:
                print(f"Connection to {self.host} established")
        except Exception as ex:
            error_msg = f"CHECK IF NEO4J IS RUNNING! While instantiating the NeoInterface object, failed to create the driver: {ex}"
            raise Exception(error_msg)

    def rdf_config(self) -> None:
        try:
            self.query("CALL n10s.graphconfig.init({handleVocabUris:'IGNORE'});")
        except:
            if self.debug:
                print("Config already created, make sure the config is correct")
        self.create_constraint(label="Resource", key="uri", type="UNIQUE", name="n10s_unique_uri")

    def rdf_setup_connection(self) -> None:
        self.rdf_config()
        if not self.rdf_host:
            self.rdf_host = os.environ.get("NEO4J_RDF_HOST")
        if not self.rdf_host:
            bolt_port = re.findall(r'\:\d+', self.host)[0]
            self.rdf_host = self.host.replace(bolt_port, ":7474").replace("bolt", "http").replace("neoj", "http")
            self.rdf_host += ("" if self.rdf_host.endswith("/") else "/") + "rdf/"
        try:
            get_response = json.loads(requests.get(f"{self.rdf_host}ping", auth=self.credentials).text)
            if self.verbose:
                if "here!" in get_response.values():
                    print(f"Connection to {self.rdf_host} established")
        except:
            error_msg = f"CHECK IF RDF ENDPOINT IS SET UP CORRECTLY! While instantiating the NeoInterface object, failed to connect to {self.rdf_host}"
            raise Exception(error_msg)

    def version(self) -> str:
        # Return the version of the Neo4j driver being used.  EXAMPLE: "4.2.1"
        return neo4j_driver_version

    def close(self) -> None:
        """
        Terminate the database connection.
        Note: this method is automatically invoked after the last operation of a "with" statement
        :return:    None
        """
        if self.driver is not None:
            self.driver.close()

    ############################################################################################
    #                                                                                          #
    #                           METHODS TO RUN GENERIC QUERIES                                 #
    #                                                                                          #
    ############################################################################################

    def query(self, q: str, params=None) -> []:
        """
        Run a general Cypher query and return a list of dictionaries.
        In cases of error, return an empty list.
        A new session to the database driver is started, and then immediately terminated after running the query.
        NOTE: if the Cypher query returns a node, and one wants to extract its internal Neo4j ID or labels
              (in addition to all the properties and their values) then use query_expanded() instead.

        :param q:       A Cypher query
        :param params:  An optional Cypher dictionary
                        EXAMPLE, assuming that the cypher string contains the substrings "$node_id":
                                {'node_id': 20}
        :return:        A (possibly empty) list of dictionaries.  Each dictionary in the list
                                will depend on the nature of the Cypher query.
                        EXAMPLES:
                            Cypher returns nodes (after finding or creating them): RETURN n1, n2
                                    -> list item such as {'n1': {'gender': 'M', 'patient_id': 123}
                                                          'n2': {'gender': 'F', 'patient_id': 444}}
                            Cypher returns attribute values that get renamed: RETURN n.gender AS client_gender, n.pid AS client_id
                                    -> list items such as {'client_gender': 'M', 'client_id': 123}
                            Cypher returns attribute values without renaming: RETURN n.gender, n.pid
                                    -> list items such as {'n.gender': 'M', 'n.pid': 123}
                            Cypher returns a single computed value
                                    -> a single list item such as {"count(n)": 100}
                            Cypher returns a single relationship, with or without attributes: MERGE (c)-[r:PAID_BY]->(p)
                                    -> a single list item such as [{ 'r': ({}, 'PAID_BY', {}) }]
                            Cypher creates nodes (without returning them)
                                    -> empty list
        """

        # Start a new session, use it, and then immediately close it
        with self.driver.session() as new_session:
            result = new_session.run(q, params)
            # Note: result is a neo4j.Result object;
            #       more specifically, an object of type neo4j.work.result.Result
            #       See https://neo4j.com/docs/api/python-driver/current/api.html#neo4j.Result
            if result is None:
                return []

            data_as_list = result.data()  # Return the result as a list of dictionaries.
            #       This must be done inside the "with" block,
            #       while the session is still open
        return data_as_list

    def query_expanded(self, q: str, params=None, flatten=False) -> []:
        """
        Expanded version of query(), meant to extract additional info for queries that return Graph Data Types,
        i.e. nodes, relationships or paths,
        such as "MATCH (n) RETURN n", or "MATCH (n1)-[r]->(n2) RETURN r"

        For example, if nodes were returned, and their Neo4j internal IDs and/or labels are desired
        (in addition to all the properties and their values)

        Unless the flatten flag is True, individual records are kept as separate lists.
            For example, "MATCH (b:boat), (c:car) RETURN b, c"
            will return a structure such as [ [b1, c1] , [b2, c2] ]  if flatten is False,
            vs.  [b1, c1, b2, c2]  if  flatten is True.  (Note: each b1, c1, etc, is a dictionary.)

        TODO:  Scenario to test:
            if b1 == b2, would that still be [b1, c1, b1(b2), c2] or [b1, c1, c2] - i.e. would we remove the duplicates?
            Try running with flatten=True "MATCH (b:boat), (c:car) RETURN b, c" on data like "CREATE (b:boat), (c1:car1), (c2:car2)"

        :param q:       A Cypher query
        :param params:  An optional Cypher dictionary
                            EXAMPLE, assuming that the cypher string contains the substring "$age":
                                        {'age': 20}
        :param flatten: Flag indicating whether the Graph Data Types need to remain clustered by record,
                        or all placed in a single flattened list.

        :return:        A (possibly empty) list of dictionaries, which will depend on which Graph Data Types
                                    were returned in the Cypher query.
                                    EXAMPLE - for a returned node
                                        {'gender': 'M', 'age': 20, 'neo4j_id': 123, 'neo4j_labels': ['patient']}
                                    EXAMPLE - for a returned relationship
                                        {'price': 7500, 'neo4j_id': 2,
                                         'neo4j_start_node': <Node id=11 labels=frozenset() properties={}>,
                                         'neo4j_end_node': <Node id=14 labels=frozenset() properties={}>,
                                         'neo4j_type': 'bought_by'}]
        """
        # Start a new session, use it, and then immediately close it
        with self.driver.session() as new_session:
            result = new_session.run(q, params)
            # Note: result is a neo4j.Result iterable object;
            #       more specifically, an object of type neo4j.work.result.Result
            #       See https://neo4j.com/docs/api/python-driver/current/api.html#neo4j.Result
            if result is None:
                return []

            data_as_list = []

            # The following must be done inside the "with" block, while the session is still open
            for record in result:
                # Note: record is a neo4j.Record object - an immutable ordered collection of key-value pairs.
                #       (the keys are the dummy names used for the nodes, such as "n")
                #       See https://neo4j.com/docs/api/python-driver/current/api.html#record

                # EXAMPLE of record (if node n was returned):
                #       <Record n=<Node id=227 labels=frozenset({'person', 'client'}) properties={'gender': 'M', 'age': 99}>>
                #       (it has one key, "n")
                # EXAMPLE of record (if node n and node c were returned):
                #       <Record n=<Node id=227 labels=frozenset({'person', 'client'}) properties={'gender': 'M', 'age': 99}>
                #               c=<Node id=66 labels=frozenset({'car'}) properties={'color': 'blue'}>>
                #       (it has 2 keys, "n" and "c")

                data = []
                for item in record:
                    # Note: item is a neo4j.graph.Node object
                    #       OR a neo4j.graph.Relationship object
                    #       OR a neo4j.graph.Path object
                    #       See https://neo4j.com/docs/api/python-driver/current/api.html#node
                    #           https://neo4j.com/docs/api/python-driver/current/api.html#relationship
                    #           https://neo4j.com/docs/api/python-driver/current/api.html#path
                    # EXAMPLES of item:
                    #       <Node id=95 labels=frozenset({'car'}) properties={'color': 'white', 'make': 'Toyota'}>
                    #       <Relationship id=12 nodes=(<Node id=147 labels=frozenset() properties={}>, <Node id=150 labels=frozenset() properties={}>) type='bought_by' properties={'price': 7500}>

                    neo4j_properties = dict(item.items())  # EXAMPLE: {'gender': 'M', 'age': 99}

                    if isinstance(item, neo4j.graph.Node):
                        neo4j_properties["neo4j_id"] = item.id  # Example: 227
                        neo4j_properties["neo4j_labels"] = list(item.labels)  # Example: ['person', 'client']

                    elif isinstance(item, neo4j.graph.Relationship):
                        neo4j_properties["neo4j_id"] = item.id  # Example: 227
                        neo4j_properties[
                            "neo4j_start_node"] = item.start_node  # A neo4j.graph.Node object with "id", "labels" and "properties"
                        neo4j_properties[
                            "neo4j_end_node"] = item.end_node  # A neo4j.graph.Node object with "id", "labels" and "properties"
                        #   Example: <Node id=118 labels=frozenset({'car'}) properties={'color': 'white'}>
                        neo4j_properties["neo4j_type"] = item.type  # The name of the relationship

                    elif isinstance(item, neo4j.graph.Path):
                        neo4j_properties["neo4j_nodes"] = item.nodes  # The sequence of Node objects in this path

                    if flatten:
                        data_as_list.append(neo4j_properties)
                    else:
                        data.append(neo4j_properties)

                if not flatten:
                    data_as_list.append(data)

            return data_as_list

    ##################################################################################################
    #                                                                                                #
    #                                    METHODS TO RETRIEVE DATA                                    #
    #                                                                                                #
    ##################################################################################################

    def get_single_field(self, field_name: str, labels="", properties_condition=None, cypher_clause=None,
                         cypher_dict=None) -> list:
        """
        For situations where one is fetching just 1 field,
        and one desires a list of those values, rather than a dictionary of records.
        In other respects, similar to the more general get_nodes()

        EXAMPLES: fetch_single_field("car", "price", properties_condition={"car_make": "Toyota"})
                        will RETURN a list of prices of all the Toyota models
                  fetch_single_field("car", "price", properties_condition={"car_make": "Toyota"}, clause="n.price < 50000")
                        will RETURN a list of prices of all the Toyota models that cost less than 50000

        :param field_name:  A string with the name of the desired field (attribute)

        For more information on the other parameters, see get_nodes()

        :return:  A list of the values of the field_name attribute in the nodes that match the specified conditions
        """

        record_list = self.get_nodes(labels, properties_condition=properties_condition,
                                     cypher_clause=cypher_clause, cypher_dict=cypher_dict)
        single_field_list = [record.get(field_name) for record in record_list]

        return single_field_list

    def get_nodes(self, labels="", properties_condition=None, cypher_clause=None, cypher_dict=None,
                  return_nodeid=False, return_labels=False) -> [{}]:
        """
        EXAMPLES:
            get_nodes("")       # Get ALL nodes
            get_nodes("client")
            get_nodes("client", properties_condition = {"gender": "M", "ethnicity": "white"})
            get_nodes("client", cypher_clause = "n.age > 40 OR n.income < 50000")
            get_nodes("client", cypher_clause = "n.age > $some_age", cypher_dict = {"$some_age": 40})
            get_nodes("client", properties_condition = {"gender": "M", "ethnicity": "white"} ,
                                           cypher_clause = "n.age > 40 OR n.income < 50000")
        RETURN a list of the records (as dictionaries of ALL the key/value node properties)
        corresponding to all the Neo4j nodes with the specified label,
            AND satisfying the given Cypher CLAUSE (if present),
            AND exactly matching ALL of the specified property key/values pairs  (if present).
            I.e. an implicit AND operation.
        IMPORTANT: nodes referred to in the Cypher clause must be specified as "n."

        A dictionary of data binding (cypher_dict) for the Cypher clause may be optionally specified.
        In case of conflict (any key overlap) between the dictionaries cypher_dict and properties_condition, and Exception is raised.
        Optionally, the Neo4j internal node ID and label name(s) may also be obtained and returned.

        :param labels:          A string (or list/tuple of strings) specifying one or more Neo4j labels;
                                    an empty string indicates that the match is to be carried out
                                    across all labels - NOT RECOMMENDED for large databases!
                                    (Note: blank spaces ARE allowed in the strings)
        :param cypher_dict:     Dictionary of data binding for the Cypher string.  EXAMPLE: {"gender": "M", "age": 40}
        :param cypher_clause:   String with a clause to refine the search; any nodes it refers to, MUST be specified as "n."
                                    EXAMPLE with hardwired values:  "n.age > 40 OR n.income < 50000"
                                    EXAMPLE with data-binding:      "n.age > $age OR n.income < $income"
                                            (data-binding values are specified in cypher_dict)
        :param properties_condition: A (possibly-empty) dictionary of property key/values pairs. Example: {"gender": "M", age: 64}
                                     IMPORTANT: cypher_dict and properties_dict must have no overlapping keys, or an Exception will be raised
        :param return_nodeid:   Flag indicating whether to also include the Neo4j internal node ID in the returned data
                                    (using "neo4j_id" as its key in the returned dictionary)
        :param return_labels:   Flag indicating whether to also include the Neo4j label names in the returned data
                                    (using "neo4j_labels" as its key in the returned dictionary)
        :return:        A list whose entries are dictionaries with each record's information
                        (the node's attribute names are the keys)
                        EXAMPLE: [  {"gender": "M", "age": 42, "condition_id": 3},
                                    {"gender": "M", "age": 76, "location": "Berkeley"}
                                 ]
                        Note that ALL the attributes of each node are returned - and that they may vary across records.
                        If the flag return_nodeid is set to True, then an extra key/value pair is included in the dictionaries,
                                of the form     "neo4j_id": some integer with the Neo4j internal node ID
                        If the flag return_labels is set to True, then an extra key/value pair is included in the dictionaries,
                                of the form     "neo4j_labels": [list of Neo4j label(s) attached to that node]
                        EXAMPLE using both of the above flags:
                            [  {"neo4j_id": 145, "neo4j_labels": ["person", "client"], "gender": "M", "age": 42, "condition_id": 3},
                               {"neo4j_id": 222, "neo4j_labels": ["person"], "gender": "M", "age": 76, "location": "Berkeley"}
                            ]
        # TODO: provide an option to specify the desired fields
        """
        (cypher, cypher_dict) = self._match_nodes(labels=labels, properties_condition=properties_condition,
                                                  cypher_clause=cypher_clause, cypher_dict=cypher_dict)
        cypher += " RETURN n"

        if self.debug:
            print(f"""
            In get_nodes().
            query: {cypher}
            parameters: {cypher_dict}
            """)

        result_list = self.query_expanded(cypher, cypher_dict, flatten=True)
        if return_nodeid and return_labels:
            # If we want to return both 'neo4j_id' and 'neo4j_labels', we're done, because query_expanded() provides both
            return result_list

        # If we get thus far, it means that either the 'neo4j_id' or the 'neo4j_labels' attribute isn't wanted;
        #   remove the unwanted one from all the dictionaries in the elements of result_list
        for node_dict in result_list:
            if not return_nodeid:
                del node_dict['neo4j_id']
            if not return_labels:
                del node_dict['neo4j_labels']

        return result_list

    def get_df(self, labels="", properties_condition=None, cypher_clause=None, cypher_dict=None,
               return_nodeid=False, return_labels=False) -> pd.DataFrame:
        """
        Same as get_nodes(), but the result is returned as a Pandas dataframe

        [See get_nodes() for information about the arguments]
        :param labels:
        :param properties_condition:
        :param cypher_clause:
        :param cypher_dict:
        :param return_nodeid:
        :param return_labels:
        :return:                A Pandas dataframe
        """
        result_list = self.get_nodes(labels=labels, properties_condition=properties_condition,
                                     cypher_clause=cypher_clause, cypher_dict=cypher_dict,
                                     return_nodeid=return_nodeid, return_labels=return_labels)
        return pd.DataFrame(result_list)

    def _match_nodes(self, labels, properties_condition=None, cypher_clause=None, cypher_dict=None) -> (str, dict):
        """
        Turn a set of specification into the MATCH part of the Cypher query, and its data-binding dictionary.

        EXAMPLES:
            _match_nodes("client")
            _match_nodes("client", properties_condition = {"gender": "M", "ethnicity": "white"})
            _match_nodes("client", cypher_clause = "n.age > 40 OR n.income < 50000")
            _match_nodes("client", cypher_clause = "n.age > $age",
                                   cypher_dict = {"$age": 40})
            _match_nodes("client", properties_condition = {"gender": "M", "ethnicity": "white"} ,
                                   cypher_clause = "n.age > 40 OR n.income < 50000")

        RETURN the MATCH part of the Cypher query, and its data-binding dictionary,
            corresponding to all the Neo4j nodes with the given label or labels (if specified),
            AND satisfying the given Cypher CLAUSE (if specified, and optionally with the data-binding cypher_dict),
            AND exactly matching ALL of the specified property key/values pairs  (if specified).
            I.e. an implicit AND operation.

        Note: cypher_dict should not contain keys of the form `par_n`, where n is an integer, or an Exception might results.

        :param labels:               A string, or list/tuple of strings, of Neo4j labels (Note: blank spaces ARE allowed)
        :param properties_condition: A (possibly-empty) dictionary of property key/values pairs.
                                     Example: {"gender": "F", "age": 22}
                                     If None or empty, no restrictions are placed on the match
        :param cypher_clause:        String with a clause to refine the search; any nodes it refers to, MUST be specified as "n."
                                     EXAMPLE with hardwired values:  "n.age > 40 OR n.income < 50000"
                                     EXAMPLE with data-binding:      "n.age > $age OR n.income < $income"
                                            (data-binding values are specified in cypher_dict)
        :param cypher_dict:          Dictionary of data binding for the Cypher string.  EXAMPLE: {"gender": "M", "age": 40}
                                     It should not contain any keys of the form `par_n`, where n is an integer
                                            (those names are reserved for internal use)

        :return:                    A pair consisting of the MATCH part of the Cypher query, and its data-binding dictionary
        """
        if properties_condition is None:
            clause_from_properties = ""
        else:
            # Transform the dictionary properties_condition into a string describing its corresponding Cypher clause,
            #       plus a corresponding data-binding dictionary.
            #       (assuming an implicit AND between equalities described by the dictionary terms),
            #
            #       EXAMPLE:
            #               properties_condition: {"gender": "F", "year first met": 2003}
            #           will lead to:
            #               clause_from_properties = "{`gender`: $par_1, `year first met`: $par_2}"
            #               props_data_binding = {'par_1': "F", 'par_2': 2003}

            (clause_from_properties, props_data_binding) = self.dict_to_cypher(properties_condition)

            if cypher_dict is None:
                cypher_dict = props_data_binding  # The properties dictionary is to be used as the Cypher-binding dictionary
            else:
                # Merge the properties dictionary into the existing cypher_dict, PROVIDED that there's no conflict
                overlap = cypher_dict.keys() & props_data_binding.keys()  # Take the set intersection
                if overlap != set():  # If not equal to the empty set
                    raise Exception(
                        f"`cypher_dict` should not contain any keys of the form `par_n` where n is an integer. "
                        f"Those names are reserved for internal use. Conflicting names: {overlap}")

                cypher_dict.update(props_data_binding)  # Merge the properties dictionary into the existing cypher_dict

        if cypher_dict is None:
            cypher_dict = {}

        if cypher_clause is not None:
            cypher_clause = cypher_clause.strip()  # Zap any leading/trailing blanks

        # Turn labels (string or list/tuple of labels) into a string suitable for inclusion into Cypher
        cypher_labels = self._prepare_labels(labels)

        # Construct the Cypher string
        cypher = f"MATCH (n {cypher_labels} {clause_from_properties})"

        if (cypher_clause != "") and (cypher_clause is not None):
            cypher += f" WHERE {cypher_clause}"

        return (cypher, cypher_dict)

    def _prepare_labels(self, labels) -> str:
        """
        Turn the given string, or list/tuple of strings - representing Neo4j labels - into a string
        suitable for inclusion in a Cypher query.
        Blanks ARE allowed in names.
        EXAMPLES:
            "client" gives rise to ":`client`"
            ["car", "car manufacturer"] gives rise to ":`car`:`car manufacturer`"


        :param labels:  A string, or list/tuple of strings, representing Neo4j labels
        :return:        A string suitable for inclusion in a Cypher query
        """
        # Turn the label strings, or list/tuple of labels, into a string suitable for inclusion into Cypher
        if labels == "":
            return ""

        if type(labels) == str:
            labels = [labels]

        cypher_labels = ""
        for single_label in labels:
            cypher_labels += f":`{single_label}`"  # EXAMPLE: ":`label 1`:`label 2`"

        return cypher_labels

    def get_parents_and_children(self, node_id: int) -> {}:
        """
        Fetch all the nodes connected to the given one by INbound relationships to it (its "parents"),
        as well as by OUTbound relationships to it (its "children")

        :param node_id: An integer with a Neo4j internal node ID
        :return:        A dictionary with 2 keys: 'parent_list' and 'child_list'
                        The values are lists of dictionaries with 3 keys: "id", "label", "rel"
                            EXAMPLE of individual items in either parent_list or child_list:
                            {'id': 163, 'labels': ['Subject'], 'rel': 'HAS_TREATMENT'}
        """
        with self.driver.session() as new_session:
            # Fetch the parents
            cypher = f"MATCH (parent)-[inbound]->(n) WHERE id(n) = {node_id} " \
                "RETURN id(parent) AS id, labels(parent) AS labels, type(inbound) AS rel"
            if self.debug:
                print(f"""
                query: {cypher}            
                """)
            result_obj = new_session.run(cypher)  # A new neo4j.Result object
            parent_list = result_obj.data()
            # EXAMPLE of parent_list:
            #       [{'id': 163, 'labels': ['Subject'], 'rel': 'HAS_TREATMENT'},
            #        {'id': 150, 'labels': ['Subject'], 'rel': 'HAS_TREATMENT'}]
            if self.verbose:
                print(f"parent_list for node {node_id}:", parent_list)

            # Fetch the children
            cypher = f"MATCH (n)-[outbound]->(child) WHERE id(n) = {node_id} " \
                "RETURN id(child) AS id, labels(child) AS labels, type(outbound) AS rel"
            if self.debug:
                print(f"""
                query: {cypher}      
                """)
            result_obj = new_session.run(cypher)  # A new neo4j.Result object
            child_list = result_obj.data()
            # EXAMPLE of child_list:
            #       [{'id': 107, 'labels': ['Source Data Row'], 'rel': 'FROM_DATA'},
            #        {'id': 103, 'labels': ['Source Data Row'], 'rel': 'FROM_DATA'}]
            if self.verbose:
                print(f"child_list for node {node_id}:", child_list)

        return {'parent_list': parent_list, 'child_list': child_list}

    def get_labels(self) -> [str]:
        """
        Extract and return a list of all the Neo4j labels present in the database.
        No particular order should be expected.
        TODO: test when there are nodes that have multiple labels
        :return:    A list of strings
        """
        results = self.query("call db.labels() yield label return label")
        return [x['label'] for x in results]

    def get_relationshipTypes(self) -> [str]:
        """
        Extract and return a list of all the Neo4j relationship types present in the database.
        No particular order should be expected.
        :return:    A list of strings
        """
        results = self.query("call db.relationshipTypes() yield relationshipType return relationshipType")
        return [x['relationshipType'] for x in results]

    def get_label_properties(self, label: str) -> list:
        q = """
        CALL db.schema.nodeTypeProperties() 
        YIELD nodeLabels, propertyName
        WHERE $label in nodeLabels and propertyName IS NOT NULL
        RETURN DISTINCT propertyName 
        ORDER BY propertyName
        """
        params = {'label': label}
        if self.debug:
            print("q : ", q, " | params : ", params)
        return [res['propertyName'] for res in self.query(q, params)]

    #########################################################################################
    #                                                                                       #
    #                           METHODS TO GET/CREATE/MODIFY SCHEMA                         #
    #                                                                                       #
    #########################################################################################

    def get_indexes(self, types=None) -> pd.DataFrame:
        """
        Return all the database indexes, and some of their attributes,
        as a Pandas dataframe.
        Optionally restrict the type (such as "BTREE") of indexes returned.

        EXAMPLE:
               labelsOrTypes            name          properties   type uniqueness
             0    [my_label]  index_23b0962b       [my_property]  BTREE  NONUNIQUE
             1    [my_label]       some_name  [another_property]  BTREE     UNIQUE

        :param types:   Optional list to of types to limit the result to
        :return:        A (possibly-empty) Pandas dataframe
        """
        if types:
            where = "with * where type in $types"  # Define a restrictive clause
        else:
            types = []
            where = ""

        q = f"""
          call db.indexes() 
          yield name, labelsOrTypes, properties, type, uniqueness
          {where}
          return *
          """

        results = self.query(q, {"types": types})
        if len(results) > 0:
            return pd.DataFrame(list(results))
        else:
            return pd.DataFrame([], columns=['name'])

    def get_constraints(self) -> pd.DataFrame:
        """
        Return all the database constraints, and some of their attributes,
        as a Pandas dataframe with 3 columns:
            name        EXAMPLE: "my_constraint"
            description EXAMPLE: "CONSTRAINT ON ( patient:patient ) ASSERT (patient.patient_id) IS UNIQUE"
            details     EXAMPLE: "Constraint( id=3, name='my_constraint', type='UNIQUENESS',
                                  schema=(:patient {patient_id}), ownedIndex=12 )"

        :return:  A (possibly-empty) Pandas dataframe
        """
        q = """
           call db.constraints() 
           yield name, description, details
           return *
           """
        results = self.query(q)
        if len(results) > 0:
            return pd.DataFrame(list(results))
        else:
            return pd.DataFrame([], columns=['name'])

    def create_index(self, label: str, key: str) -> bool:
        """
        Create a new database index, unless it already exists,
        to be applied to the specified label and key (property).
        The standard name given to the new index is of the form label.key

        EXAMPLE - to index nodes labeled "car" by their key "color":
                        create_index("car", "color")
                  This new index - if not already in existence - will be named "car.color"

        If an existing index entry contains a list of labels (or types) such as ["l1", "l2"] ,
        and a list of properties such as ["p1", "p2"] ,
        then the given pair (label, key) is checked against ("l1_l2", "p1_p2"), to decide whether it already exists.

        :param label:   A string with the node label to which the index is to be applied
        :param key:     A string with the key (property) name to which the index is to be applied
        :return:        True if a new index was created, or False otherwise
        """
        existing_indexes = self.get_indexes()  # A Pandas dataframe with info about indexes;
        #       in particular 2 columns named "labelsOrTypes" and "properties"

        # Index is created if not already exists.
        # a standard name for the index is assigned: `{label}.{key}`
        existing_standard_name_pairs = list(existing_indexes.apply(
            lambda x: ("_".join(x['labelsOrTypes']), "_".join(x['properties'])), axis=1))  # Proceed by row
        """
        For example, if the Pandas dataframe existing_indexes contains the following columns: 
                            labelsOrTypes     properties
                0                   [car]  [color, make]
                1                [person]          [sex]
                
        then existing_standard_names will be:  [('car', 'color_make'), ('person', 'sex')]
        """

        if (label, key) not in existing_standard_name_pairs:
            q = f'CREATE INDEX `{label}.{key}` FOR (s:`{label}`) ON (s.`{key}`)'
            if self.debug:
                print(f"""
                query: {q}
                """)
            self.query(q)
            return True
        else:
            return False

    def create_constraint(self, label: str, key: str, type="UNIQUE", name=None) -> bool:
        """
        Create a uniqueness constraint for a node property in the graph,
        unless a constraint with the standard name of the form `{label}.{key}.{type}` is already present

        Note: it also creates an index, and cannot be applied if an index already exists.

        EXAMPLE: create_constraint("patient", "patient_id")

        :param label:   A string with the node label to which the constraint is to be applied
        :param key:     A string with the key (property) name to which the constraint is to be applied
        :param type:    For now, the default "UNIQUE" is the only allowed option
        :param name:    Optional name to give to the new constraint; if not provided, a
                            standard name of the form `{label}.{key}.{type}` is used.  EXAMPLE: "patient.patient_id.UNIQUE"
        :return:        True if a new constraint was created, or False otherwise
        """
        assert type == "UNIQUE"
        # TODO: consider other types of constraints

        existing_constraints = self.get_constraints()
        # constraint is created if not already exists.
        # a standard name for a constraint is assigned: `{label}.{key}.{type}` if name was not provided
        cname = (name if name else f"`{label}.{key}.{type}`")
        if cname in list(existing_constraints['name']):
            return False

        try:
            q = f'CREATE CONSTRAINT {cname} ON (s:`{label}`) ASSERT s.`{key}` IS UNIQUE'
            if self.debug:
                print(f"""
                query: {q}
                """)
            self.query(q)
            # Note: creation of a constraint will crash if another constraint, or index, already exists
            #           for the specified label and key
            return True
        except Exception:
            return False

    def drop_index(self, name: str) -> bool:
        """
        Eliminate the index with the specified name.

        :param name:    Name of the index to eliminate
        :return:        True if successful or False otherwise (for example, if the index doesn't exist)
        """
        try:
            q = f"DROP INDEX `{name}`"
            if self.debug:
                print(f"""
                query: {q}
                """)
            self.query(q)  # Note: it crashes if the index doesn't exist
            return True
        except Exception:
            return False

    def drop_all_indexes(self, including_constraints=True) -> None:
        """
        Eliminate all the indexes in the database and, optionally, also get rid of all constraints

        :param including_constraints:   Flag indicating whether to also ditch all the constraints
        :return:                        None
        """
        if including_constraints:
            if self.apoc:
                self.query("call apoc.schema.assert({},{})")
            else:
                self.drop_all_constraints()

        indexes = self.get_indexes()
        for name in indexes['name']:
            self.drop_index(name)

    def drop_constraint(self, name: str) -> bool:
        """
        Eliminate the constraint with the specified name.

        :param name:    Name of the constraint to eliminate
        :return:        True if successful or False otherwise (for example, if the constraint doesn't exist)
        """
        try:
            q = f"DROP CONSTRAINT `{name}`"
            if self.debug:
                print(f"""
                query: {q}
                """)
            self.query(q)  # Note: it crashes if the constraint doesn't exist
            return True
        except Exception:
            return False

    def drop_all_constraints(self) -> None:
        """
        Eliminate all the constraints in the database

        :return:    None
        """
        constraints = self.get_constraints()
        for name in constraints['name']:
            if not (self.rdf and name == 'n10s_unique_uri'):
                self.drop_constraint(name)

    #####################################################################################
    #                                                                                   #
    #                           METHODS TO CREATE/MODIFY DATA                           #
    #                                                                                   #
    #####################################################################################

    def create_node_by_label_and_dict(self, labels, properties=None) -> int:
        """
        Create a new node with the given label and with the attributes/values specified in the items dictionary
        Return the Neo4j internal ID of the node just created.

        :param labels:      A string, or list/tuple of strings, of Neo4j label (ok to include blank spaces)
        :param properties:  An optional dictionary of properties to set for the new node.
                                EXAMPLE: {'age': 22, 'gender': 'F'}

        :return:            An integer with the Neo4j internal ID of the node just created
        """

        if properties is None:
            properties = {}

        # From the dictionary of attribute names/values,
        #       create a part of a Cypher query, with its accompanying data dictionary
        (attributes_str, data_dictionary) = self.dict_to_cypher(properties)
        # EXAMPLE:
        #       attributes_str = '{`cost`: $par_1, `item description`: $par_2}'
        #       data_dictionary = {'par_1': 65.99, 'par_2': 'the "red" button'}

        # Turn labels (string or list/tuple of labels) into a string suitable for inclusion into Cypher
        cypher_labels = self._prepare_labels(labels)

        # Assemble the complete Cypher query
        cypher = f"CREATE (n {cypher_labels} {attributes_str}) RETURN n"

        if self.debug:
            print(f"""
                In create_node_by_label_and_dict().
                query: {cypher}
                parameters: {data_dictionary}
                """)

        result_list = self.query_expanded(cypher, data_dictionary, flatten=True)
        return result_list[0]['neo4j_id']  # Return the Neo4j internal ID of the node just created

    def delete_nodes_by_label(self, delete_labels=None, keep_labels=None, batch_size=50000) -> None:
        """
        Empty out (by default completely) the Neo4j database.
        Optionally, only delete nodes with the specified labels, or only keep nodes with the given labels.
        Note: the keep_labels list has higher priority; if a label occurs in both lists, it will be kept.
        IMPORTANT: it does NOT clear indexes; "ghost" labels may remain!  To get rid of those, run drop_all_indexes()

        :param delete_labels:   An optional string, or list of strings, indicating specific labels to DELETE
        :param keep_labels:     An optional string or list of strings, indicating specific labels to KEEP
                                    (keep_labels has higher priority over delete_labels)
        :return:                None
        """
        if (delete_labels is None) and (keep_labels is None):
            # Delete ALL nodes AND ALL relationship from the database; for efficiency, do it all at once
            if self.verbose:
                print(f" --- Deleting all nodes in the database ---")

            if batch_size:  # In order to avoid memory errors, delete data in batches
                q = f"""
                      call apoc.periodic.iterate(
                      'MATCH (n) RETURN n',
                      'DETACH DELETE(n)',        
                      {{batchSize:{str(batch_size)}, parallel:false}})
                      YIELD total, batches, failedBatches
                      RETURN total, batches, failedBatches                                                                    
                     """
            else:
                q = "MATCH (n) DETACH DELETE(n)"

            if self.debug:
                print(f"""
                query: {q}
                """)

            self.query(q)
            return

        if not delete_labels:
            delete_labels = self.get_labels()  # If no specific labels to delete were given,
            # then consider all labels for possible deletion (unless marked as "keep", below)
        else:
            if type(delete_labels) == str:
                delete_labels = [delete_labels]  # If a string was passed, turn it into a list

        if not keep_labels:
            keep_labels = []  # Initialize list of labels to keep, if not provided
        else:
            if type(keep_labels) == str:
                keep_labels = [keep_labels]  # If a string was passed, turn it into a list

        # Delete all nodes with labels in the delete_labels list,
        #   EXCEPT for any label in the keep_labels list
        for label in delete_labels:
            if not (label in keep_labels):
                if self.verbose:
                    print(f" --- Deleting nodes with label: `{label}` ---")
                q = f"MATCH (x:`{label}`) DETACH DELETE x"
                if self.debug:
                    print(f"""
                    query: {q}                        
                    """)
                self.query(q)

    def clean_slate(self, keep_labels=None, drop_indexes=True, drop_constraints=True) -> None:
        """
        Use this to get rid of absolutely everything in the database.
        Optionally, keep nodes with a given label, or keep the indexes, or keep the constraints
        :param keep_labels:     An optional list of strings, indicating specific labels to KEEP
        :param drop_indexes:    Flag indicating whether to also ditch all indexes (by default, True)
        :param drop_constraints:Flag indicating whether to also ditch all constraints (by default, True)
        :return:                None
        """
        if self.rdf:
            self.delete_nodes_by_label(
                keep_labels=(keep_labels + ['_GraphConfig'] if keep_labels else ['_GraphConfig']))
        else:
            self.delete_nodes_by_label(keep_labels=keep_labels)

        if drop_indexes:
            self.drop_all_indexes(including_constraints=drop_constraints)

    def set_fields(self, labels, set_dict, properties_condition=None, cypher_clause=None, cypher_dict=None) -> None:
        """
        EXAMPLE - locate the "car" with vehicle id 123 and set its color to white and price to 7000
            set_fields(labels = "car", set_dict = {"color": "white", "price": 7000},
                       properties_condition = {"vehicle id": 123})

        LIMITATION: blanks are allowed in the keys of properties_condition, but not in those of set_dict

        :param labels:                  A string, or list/tuple of strings, representing Neo4j labels
        :param set_dict:                A dictionary of field name/values to create/update the node's attributes
                                            (note: no blanks are allowed in the keys)
        :param properties_condition:
        :param cypher_clause:
        :param cypher_dict:
        :return:                        None
        """

        (cypher_match, cypher_dict) = self._match_nodes(labels, properties_condition=properties_condition,
                                                        cypher_clause=cypher_clause, cypher_dict=cypher_dict)

        set_list = []
        for field_name, field_value in set_dict.items():  # field_name, field_value are key/values in set_dict
            set_list.append("n.`" + field_name + "` = $" + field_name)  # Example:  "n.`field1` = $field1"
            cypher_dict[field_name] = field_value  # Extend the Cypher data-binding dictionary

        # Example of data_binding at the end of the loop: {"field1": value1, "field2": value2}

        set_clause = "SET " + ", ".join(set_list)  # Example:  "SET n.field1 = $field1, n.field2 = $field2"

        cypher = cypher_match + set_clause

        # Example of cypher:
        # "MATCH (n:car {`vehicle id`: $par_1}) SET n.`color` = color, n.`price` = $field2"
        # Example of data binding:
        #       {"par_1": 123, "color": "white", "price": 7000}

        if self.debug:
            print("cypher: ", cypher)
            print("data_binding: ", cypher_dict)

        self.query(cypher, cypher_dict)

    def extract_entities(self,
                         mode='merge',
                         label=None,
                         cypher=None,
                         cypher_dict=None,
                         target_label=None,
                         property_mapping={},
                         relationship=None,
                         direction='<'
                         ):
        """
        :param mode:str; assert mode in ['merge', 'create']
        :param label:str; label of the nodes to extract data from
        :param cypher: str; only of label not provided: cypher that returns id(node) of the nodes to extract data from
        EXAMPLE:
        cypher = '''
        MATCH (f:`Source Data Table`{{_domain_:$domain}})-[:HAS_DATA]->(node:`Source Data Row`)
        RETURN id(node)
        '''
        :param cypher_dict: None/dict parameters required for the cypher query
        EXAMPLE:
        cypher_dict={'domain':'ADSL'}
        :param target_label: label(s) of the newly created nodes with extracted data: list or str
        :param property_mapping: dict or list
            if dict: keys correspond to the property names of source data (e.g. Source Data Row) and values correspond
            to to the property names of the target class where the data is extracted to
            		if list: properties of the extracted node (as per the list) will extracted and will be named same as
            		in the source node
        :param relationship: type of the relationship (to/from the extraction node) to create
        :param direction: direction of the relationship to create (>: to the extraction node, <: from the extraction node)
        :return: None
        """
        assert mode in ['merge', 'create']
        assert direction in ['>', '<']
        assert type(property_mapping) in [dict, list]
        assert type(target_label) in [list, str] or target_label is None
        if target_label:
            if type(target_label) == str:
                target_label = [target_label]
        if type(property_mapping) == list:
            property_mapping = {k: k for k in property_mapping}
        for key in property_mapping.keys():
            self.create_index(label, key)
        q_match_part = f"MATCH (data:`{label}`) RETURN data"
        q_match_altered = False
        if cypher:
            if not cypher_dict:
                cypher_dict = {}
            all = [x[1:] for x in re.findall(r'\$\w+\b', cypher)]
            missing_params = set(all) - set(cypher_dict.keys())
            if not missing_params:
                q_match_part = """
                CALL apoc.cypher.run($cypher, $cypher_dict) YIELD value
                MATCH (data) WHERE id(data) = value['id(node)'] 
                RETURN data                               
                """
                q_match_altered = True
            else:
                if self.debug:
                    print("ERROR: not all parameters have been supplied in cypher_dict, missing: ", missing_params)

        rel_left = ('' if direction == '>' else '<')
        rel_right = ('>' if direction == '>' else '')
        q = f"""
                    call apoc.periodic.iterate(
                   $q_match_part
                   ,
                   '
                       WITH data, apoc.coll.intersection(keys($mapping), keys(data)) as common_keys
                       {("" if mode == "create" else "WHERE size(common_keys) > 0")}
                       WITH data, apoc.map.fromLists([key in common_keys | $mapping[key]], [key in common_keys | data[key]]) as submap                                               
                       call apoc.{mode}.node($target_label, submap) YIELD node MERGE (data){rel_left}-[:`{relationship}`]-{rel_right}(node)
                   ',        
                   {{batchSize:10000, parallel:false, params: $inner_params}})
                   YIELD total, batches, failedBatches
                   RETURN total, batches, failedBatches                                                                    
               """
        inner_params = {'target_label': target_label,
                        'mapping': property_mapping}
        if q_match_altered:
            inner_params = {**inner_params, 'cypher': cypher, 'cypher_dict': cypher_dict}
        params = {'q_match_part': q_match_part, 'target_label': target_label, 'inner_params': inner_params}
        res = self.query(q, params)
        if self.debug:
            print("        Query : ", q)
            print("        Query parameters: ", params)
            print("        Result of above query : ", res, "\n")

    #########################################################################################
    #                                                                                       #
    #                           METHODS TO CREATE NEW RELATIONSHIPS                         #
    #                                                                                       #
    #########################################################################################

    def link_entities(self,
                      left_class: str,
                      right_class: str,
                      relationship="_default_",
                      cond_via_node=None,
                      cond_left_rel=None,
                      cond_right_rel=None,
                      cond_cypher=None,
                      cond_cypher_dict=None):
        """
        Creates relationship of type {relationship} ...
        :param left_class:      Name of the left class
        :param right_class:     Name of the right class
        :param relationship:    Name to give the relationship (if None: will use name of right_class (f'HAS_{right_class.upper())')
        :param cond_via_node:   Name of central node from which relationships will be created
        :param cond_left_rel:   Name and direction of relationship from right_class (e.g. FROM_DATA> or <FROM_DATA)
        :param cond_right_rel:  Name and direction of relationship from left_class (e.g. FROM_DATA> or <FROM_DATA)
        :param cond_cypher:     (optional) - if not None: cond_via_node, cond_left_rel, cond_right_rel will be ignored
                                instead the cypher query will be run which return nodes 'left' and 'right' to be linked
                                with relationship of type {relationship}
        :param cond_cypher_dict: parameters required for the cypher query
        """
        # checking compliance of provided parameters
        if not cond_cypher:
            assert not (cond_left_rel.startswith("<") and cond_left_rel.endswith(">"))
            assert not (cond_right_rel.startswith("<") and cond_right_rel.endswith(">"))
        if relationship == '_default_':
            relationship = f'HAS_{right_class.upper()}'
        cond_left_rel_mark1 = ""
        cond_left_rel_mark2 = ""
        if cond_left_rel.startswith("<"):
            cond_left_rel_mark1 = "<"
        if cond_left_rel.endswith(">"):
            cond_left_rel_mark2 = ">"
        cond_left_rel_type = re.sub(r'^(\<)?(.*?)(\>)?$', r'\2', cond_left_rel)
        cond_right_rel_mark1 = ""
        cond_right_rel_mark2 = ""
        if cond_right_rel.startswith("<"):
            cond_right_rel_mark1 = "<"
        if cond_right_rel.endswith(">"):
            cond_right_rel_mark2 = ">"
        cond_right_rel_type = re.sub(r'^(\<)?(.*?)(\>)?$', r'\2', cond_right_rel)
        if cond_cypher:
            if self.verbose:
                print(
                    f"Using cypher condition to link nodes. Labels: {left_class}, {right_class}; Cypher: {cond_cypher}")
                periodic_part1 = """
                CALL apoc.cypher.run($cypher, $cypher_dict) YIELD value
                RETURN value.`left` as left, value.`right` as right                                                
                """
        else:
            periodic_part1 = f'''
            MATCH (left){cond_left_rel_mark1}-[:`{cond_left_rel_type}`*0..1]-{cond_left_rel_mark2}(sdr:`{cond_via_node}`),
            (sdr){cond_right_rel_mark1}-[:`{cond_right_rel_type}`*0..1]-{cond_right_rel_mark2}(right)
            WHERE left:`{left_class}` and right:`{right_class}` 
            RETURN left, right 
            '''
        q = f"""              
            call apoc.periodic.iterate(
            '{periodic_part1}',
            '
               MERGE (left)-[:`{relationship}`]->(right)          
            ',        
            {{batchSize:10000, parallel:false, params: {{cypher: $cypher, cypher_dict: $cypher_dict}}}})
            YIELD total, batches, failedBatches
            RETURN total, batches, failedBatches
        """
        params = {'cypher': cond_cypher, 'cypher_dict': cond_cypher_dict}
        if self.debug:
            print("        Query : ", q)
            print("        Query parameters: ", params)
        self.query(q, params)

    def link_nodes_on_matching_property(self, label1: str, label2: str, property1: str, rel: str,
                                        property2=None) -> None:
        """
        Locate any pair of Neo4j nodes where all of the following hold:
                            1) the first one has label1
                            2) the second one has label2
                            3) the two nodes agree in the value of property1 (if property2 is None),
                                        or in the values of property1 in the 1st node and property2 in the 2nd node

        For any such pair found, add a relationship - with the name specified in the rel argument - from the 1st to 2nd node,
        unless already present

        :param label1:      A string against which the label of the 1st node must match
        :param label2:      A string against which the label of the 2nd node must match
        :param property1:   Name of property that must be present in the 1st node (and also in 2nd node, if property2 is None)
        :param property2:   Name of property that must be present in the 2nd node (may be None)
        :param rel:         Name to give to all relationships that get created
        :return:            None
        """
        if not property2:
            property2 = property1
        q = f'''MATCH (x:`{label1}`), (y:`{label2}`) WHERE x.`{property1}` = y.`{property2}` 
                MERGE (x)-[:{rel}]->(y)'''
        if self.debug:
            print(f"""
            query: {q}
            """)
        self.query(q)

    def link_nodes_on_matching_property_value(self, label1: str, label2: str, prop_name: str, prop_value: str,
                                              rel: str) -> None:
        """
        Locate any pair of Neo4j nodes where all of the following hold:
                            1) the first one has label1
                            2) the second one has label2
                            3) both nodes have a property with the specified name
                            4) the string values of the properties in (3) in the two nodes are both equal to the specified value
        For any such pair found, add a relationship - with the name specified in the rel argument - from the 1st to 2nd node,
        unless already present

        :param label1:      A string against which the label of the 1st node must match
        :param label2:      A string against which the label of the 2nd node must match
        :param prop_name:   Name of property that must be present in both nodes
        :param prop_value:  A STRING value that the above property must have in both nodes
        :param rel:         Name to give to all relationships that get created
        :return:            None
        """
        q = f'''MATCH (x:`{label1}`), (y:`{label2}`) WHERE x.`{prop_name}` = "{prop_value}" AND y.`{prop_name}` = "{prop_value}" 
                MERGE (x)-[:{rel}]->(y)'''
        if self.debug:
            print(f"""
            query: {q}
            """)
        self.query(q)

    def link_nodes_by_ids(self, node_id1: int, node_id2: int, rel: str, rel_props=None) -> None:
        """
        Locate the pair of Neo4j nodes with the given Neo4j internal ID's.
        If they are found, add a relationship - with the name specified in the rel argument,
        and with the specified optional properties - from the 1st to 2nd node, unless already present
        TODO: maybe return the Neo4j ID of the relationship just created

        :param node_id1:    An integer with the Neo4j internal ID to locate the 1st node
        :param node_id2:    An integer with the Neo4j internal ID to locate the 2nd node
        :param rel:         A string specifying a Neo4j relationship name
        :param rel_props:   Optional dictionary with the relationship properties.  EXAMPLE: {'since': 2003, 'code': 'xyz'}
        :return:            None
        """

        cypher_rel_props, cypher_dict = self.dict_to_cypher(rel_props)  # Process the optional relationship properties
        # EXAMPLE of cypher_rel_props: '{cost: $par_1, code: $par_2}'   (possibly blank)
        # EXAMPLE of cypher_dict: {'par_1': 65.99, 'par_2': 'xyz'}      (possibly empty)

        q = f"""
        MATCH (x), (y) 
        WHERE id(x) = $node_id1 and id(y) = $node_id2
        MERGE (x)-[:`{rel}` {cypher_rel_props}]->(y)
        """

        # Extend the (possibly empty) Cypher data dictionary, to also include a value for the key "node_id1" and "node_id2"
        cypher_dict["node_id1"] = node_id1
        cypher_dict["node_id2"] = node_id2

        if self.debug:
            print(f"""
            query: {q}
            parameters: {cypher_dict}
            """)

        self.query(q, cypher_dict)

    #####################################################################################################
    #                                                                                                   #
    #                                   METHODS TO READ IN DATA                                         #
    #                                                                                                   #
    #####################################################################################################

    def load_df(
            self,
            df: pd.DataFrame,
            label: str,
            merge=False,
            primary_key=None,
            merge_overwrite=False,
            rename=None,
            max_chunk_size=10000) -> list:
        """
        Load a Pandas data frame into Neo4j.
        Each line is loaded as a separate node.
        TODO: maybe save the Panda data frame's row number as an attribute of the Neo4j nodes, to ALWAYS have a primary key

        :param df:              A Pandas data frame to import into Neo4j
        :param label:           String with a Neo4j label to use on the newly-created nodes
        :param merge:           If True, records are replaced, rather than added, if already present;
                                if False, always added
        :param primary_key:     Only applicable when merging.  String with the name of the field that
                                serves as a primary key; if a new record with that field is to be added,
                                it'll replace the current one
        TODO: to allow for list of primary_keys
        :param merge_overwrite: If True then on merge the existing nodes will be overwritten with the new data,
                                otherwise they will be updated with new information (keys that are not present in the df
                                will not be deleted)
        :param rename:          Optional dictionary to rename the Pandas dataframe's columns to
                                    EXAMPLE {"current_name": "name_we_want"}
        :param max_chunk_size:  To limit the number of rows loaded at one time
        :return:                List of node ids, created in the operation
        """
        if isinstance(df, pd.Series):
            df = pd.DataFrame(df)
        if rename is not None:
            df = df.rename(rename, axis=1)  # Rename the columns in the Pandas data frame

        primary_key_s = ''
        if primary_key is not None:
            neo_indexes = self.get_indexes()
            if label not in list(neo_indexes['name']):
                self.create_index(label, primary_key)
            primary_key_s = '{' + f'`{primary_key}`:record[\'{primary_key}\']' + '}'
            # EXAMPLE of primary_key_s: "{patient_id:record['patient_id']}"

        op = 'MERGE' if (merge and primary_key) else 'CREATE'  # A MERGE or CREATE operation, as needed
        res = []
        for df_chunk in np.array_split(df, int(len(df.index) / max_chunk_size) + 1):  # Split the operation into batches
            cypher = f'''
            WITH $data AS data 
            UNWIND data AS record {op} (x:`{label}`{primary_key_s}) 
            SET x{('' if merge_overwrite else '+')}=record 
            RETURN id(x) as node_id 
            '''
            cypher_dict = {'data': df_chunk.to_dict(orient='records')}
            if self.debug:
                print(f"""
                query: {cypher}
                parameters: {cypher_dict}
                """)
            res_chunk = self.query(cypher, cypher_dict)
            if res_chunk:
                res += [r['node_id'] for r in res_chunk]
        return res

    def load_dict(self, dct: dict, label="Root", rel_prefix="", maxdepth=10):
        """
        Loads python dict to Neo4j (auto-unpacking hierarchy)
        Children of type dict converted into related nodes with relationship {rel_prefix}_{key}
        Children of type list (of dict or other) converted into:
            - multiple related nodes for list items of type dict
            - properties of parent node of type list in case list items
        see example in tests.test_json.test_import_custom_json
        :param dct: python dict to load
        :param label: label to assign to the root node
        :param rel_prefix: prefix to add to relationship name from parent to child
        :param maxdepth: maximum possible depth(of children) of dict
        :return: None
        """
        # initial load of the complete json as a node
        j = json.dumps(dct)
        self.query(
            """
            CALL apoc.merge.node(['JSON',$label],{value:$value}) 
            YIELD node
            RETURN node            
            """
            ,
            {'label': label, 'value': j}
        )
        i = 0
        # unpacking hierarchy (looping until no nodes with JSON label are left or maxdepth reached
        while (self.query("MATCH (j:JSON) RETURN j LIMIT 1")) and i < maxdepth:
            self.query("""
                MATCH (j:JSON)
                WITH j, apoc.convert.fromJsonMap(j.value) as map
                WITH j, map, keys(map) as ks UNWIND ks as k
                call apoc.do.case([
                    apoc.meta.type(map[k]) = 'MAP'
                    ,
                    '
                    CALL apoc.merge.node(["JSON", $k], {value: apoc.convert.toJson($map[$k])}) YIELD node            
                    CALL apoc.merge.relationship(j,$rel_prefix + k, {}, {}, node, {}) YIELD rel            
                    RETURN node, rel       
                    '
                    ,
                    apoc.meta.type(map[k]) = 'LIST'
                    ,
                    '
                    //first setting LIST property on main node                    
                    WITH j, map, k, [i in map[k] WHERE apoc.meta.type(i) <> "MAP"] as not_map_lst
                    call apoc.do.when(
                        size(not_map_lst) <> 0,
                        "call apoc.create.setProperty([j], $k, $not_map_lst) YIELD node RETURN node",
                        "RETURN j",
                        {j:j, k:k, not_map_lst:not_map_lst}
                    ) YIELD value
                    WITH *, [i in map[k] WHERE NOT i IN not_map_lst] as map_lst                    
                    UNWIND map_lst as item_map
                    CALL apoc.merge.node(["JSON", $k], {value: apoc.convert.toJson(item_map)}) YIELD node            
                    CALL apoc.merge.relationship(j,$rel_prefix + k, {}, {}, node, {}) YIELD rel            
                    RETURN node, rel   
                    '   
                    ]
                    ,
                    '
                    call apoc.create.setProperty([j], $k, $map[$k]) YIELD node
                    RETURN node         
                    '  
                    ,
                    {k: k, map: map, j: j, rel_prefix: $rel_prefix}        
                ) YIELD value
                WITH DISTINCT j
                REMOVE j:JSON  
                REMOVE j.value  
                """, {"rel_prefix": rel_prefix})
            i += 1

    def load_arrows_dict(self, dct: dict, merge_on=None, always_create=None, timestamp=False):
        """
        Loads data created in prototyping tool https://arrows.app/
        Uses MERGE statement separately on each node and each relationship using all properties as identifying properties
        Example of use:
        with open("arrows.json", 'r') as jsonfile:
            dct = json.load(jsonfile)
        neo = NeoInterface()
        neo.load_arrows_dict(dct)

        :param dct: python dict to load
        :param merge_on: None or dict with label as key and list of properties as value - the properties will be used
        as identProps in apoc.merge.node, the rest of the properties will be used as onCreateProps and onMatchProps
        :return: result of the corresponding Neo4j query
        """
        assert merge_on is None or isinstance(merge_on, dict)
        if not merge_on:
            merge_on = {}
        for key, item in merge_on.items():
            assert isinstance(item, list)
        assert always_create is None or isinstance(always_create, list)
        # if merge_on:
        q = """
            UNWIND $map['nodes'] as nd
            WITH *, apoc.coll.intersection(nd['labels'], keys($merge_on)) as hc_labels // list of relevant labels from the merge_on map
            WITH *, apoc.coll.toSet(apoc.coll.flatten(apoc.map.values($merge_on, hc_labels))) as hc_props // list of relevant properties 
            WITH *, [prop in hc_props WHERE prop in keys(nd['properties'])] as hc_props // filter to keep only the existing ones
            WITH 
                *,
                CASE WHEN size(nd['labels']) = 0 THEN 
                    ['No Label']
                ELSE
                    nd['labels']
                END as labels,
                CASE WHEN size(hc_props) > 0 THEN 
                    {
                        identProps: 
                            CASE WHEN size(apoc.coll.intersection(keys(nd['properties']), hc_props)) = 0 and nd['caption'] <> '' THEN 
                                {value: nd['caption']}
                            ELSE
                                apoc.map.submap(nd['properties'], hc_props)
                            END
                        ,
                        onMatchProps: apoc.map.submap(nd['properties'], [key in keys(nd['properties']) 
                                                                         WHERE NOT key IN hc_props])
                    }
                ELSE
                    {
                        identProps: 
                            CASE WHEN size(keys(nd['properties'])) = 0 and nd['caption'] <> '' THEN 
                                {value: nd['caption']}
                            ELSE
                                nd['properties']
                            END
                        ,
                        onMatchProps: {}
                    }                   
                END as props                        
            WITH 
                nd,
                labels,
                props['identProps'] as identProps,
                props['onMatchProps'] as onMatchProps,
                props['onMatchProps'] as onCreateProps //TODO: change if these need to differ in the future
            //dummy property if no properties are ident                                          
            WITH *, CASE WHEN identProps = {} THEN {_dummy_prop_:1} ELSE identProps END as identProps 
        """ + \
        ("""
        WITH 
            *, 
            apoc.map.mergeList([onCreateProps, {_timestamp: timestamp()}]) as onCreateProps,
            apoc.map.mergeList([onMatchProps, {_timestamp: timestamp()}]) as onMatchProps
        """ if timestamp else "") + \
        """
            CALL apoc.do.when(
                size(apoc.coll.intersection(labels, $always_create)) > 0,    
                "CALL apoc.create.node($labels, apoc.map.mergeList([$identProps, $onMatchProps, $onCreateProps])) YIELD node RETURN node",
                "CALL apoc.merge.node($labels, $identProps, $onMatchProps, $onCreateProps) YIELD node RETURN node",
                {labels: labels, identProps:identProps, onMatchProps:onMatchProps, onCreateProps:onCreateProps}
            ) yield value as value2
            WITH *, value2['node'] as node
            //eliminating dummy property
            CALL apoc.do.when( 
                identProps = {_dummy_prop_: 1},
                'REMOVE node._dummy_prop_ RETURN node',
                'RETURN node',
                {node: node}
            ) YIELD value
            WITH * 
            WITH apoc.map.fromPairs(collect([nd['id'], node])) as node_map
            UNWIND $map['relationships'] as rel                   
            call apoc.merge.relationship(
                node_map[rel['fromId']], 
                CASE WHEN rel['type'] = '' OR rel['type'] IS NULL THEN 'RELATED' ELSE rel['type'] END, 
                rel['properties'], 
                {}, 
                node_map[rel['toId']], {}
            )
            YIELD rel as relationship
            WITH node_map, apoc.map.fromPairs(collect([rel['id'], relationship])) as rel_map
            RETURN node_map, rel_map
            """
        params = {
            'map': dct,
            'merge_on': (merge_on if merge_on else {}),
            'always_create': (always_create if always_create else [])
        }
        res = self.query(q, params)
        if res:
            return res[0]
        else:
            return None

    ############################################################################################
    #                                                                                          #
    #                               UTILITY METHODS                                            #
    #                                                                                          #
    ############################################################################################

    def dict_to_cypher(self, data_dict: {}) -> (str, {}):
        """
        Turn a Python dictionary (meant for specifying node or relationship attributes)
        into a string suitable for Cypher queries,
        plus its corresponding data-binding dictionary.

        EXAMPLE :
                    {'cost': 65.99, 'item description': 'the "red" button'}
                will lead to
                    (
                        '{`cost`: $par_1, `item description`: $par_2}',
                        {'par_1': 65.99, 'par_2': 'the "red" button'}
                    )

        Note that backticks are used in the Cypher string to allow blanks in the key names.
        Consecutively-named dummy variables ($par_1, $par_2, etc) are used,
        instead of names based on the keys of the data dictionary (such as $cost),
        because the keys might contain blanks.

        :param data_dict:   A Python dictionary
        :return:            A pair consisting of a string suitable for Cypher queries,
                                and a corresponding data-binding dictionary.
                            If the passed dictionary is empty or None,
                                the pair returned is ("", {})
        """
        if data_dict is None or data_dict == {}:
            return ("", {})

        rel_props_list = []  # A list of strings
        data_dictionary = {}
        parameter_count = 1  # Sequential integers used in the data dictionary, such as "par_1", "par_2", etc.
        for prop_key, prop_value in data_dict.items():
            parameter_token = f"par_{parameter_count}"  # EXAMPLE: "par_3"

            # Extend the list of Cypher property relationships and their corresponding data dictionary
            rel_props_list.append(f"`{prop_key}`: ${parameter_token}")  # The $ refers to the data binding
            data_dictionary[parameter_token] = prop_value
            parameter_count += 1

        rel_props_str = ", ".join(rel_props_list)

        rel_props_str = "{" + rel_props_str + "}"

        return (rel_props_str, data_dictionary)

    ############################################################################################
    #                                                                                          #
    #                           METHODS TO SUPPORT DEBUGGING                                   #
    #                                                                                          #
    ############################################################################################

    def neo4j_query_params_from_dict(self, params: dict, char_limit=500) -> str:
        """
        Given a Python dictionary, meant to represent value/key pairs,
        compose and return a string suitable for pasting into the Neo4j browser, for testing purposes.

        EXAMPLE:            {'age': 22, 'gender': 'F'}
                    will produce the string
                            :param age=> 22;
                            :param gender=> 'F';

        :param params:     query parameters in the form of python dict
        :param char_limit: limit number of characters to include in each line
        :return:           string of parameters to paste into Neo4j browser for testing procedures in the browser
        """
        s = ""  # String suitable for pasting into the Neo4j browser
        for key, item in params.items():
            prefix = "".join([":param ", key, "=> "])

            if type(item) == int:
                res = ("".join([prefix, str(item), ";"]))
            elif type(item) == dict:
                cypher_dict = "".join(["apoc.map.fromPairs([" + ",".join(
                    [f"['{key2}', {item2}]" for key2, item2 in item.items()]) + "])"])
                res = ("".join([prefix, cypher_dict, ";"]))
            else:
                res = ("".join([prefix, "".join(['\'', str(item), '\'']), ";"]))

            s += res[:char_limit] + "\n"

        return s

    ############################################################################################
    #                                                                                          #
    #                           METHODS TO SUPPORT JSON IMPORT/EXPORT                          #
    #                                                                                          #
    ############################################################################################

    def export_dbase_json(self) -> {}:
        """
        Export the entire Neo4j database as a JSON string
        EXAMPLE:
        { 'nodes': 2,
          'relationships': 1,
          'properties': 6,
          'data': '[{"type":"node","id":"3","labels":["User"],"properties":{"name":"Adam","age":32,"male":true}},\n
                    {"type":"node","id":"4","labels":["User"],"properties":{"name":"Eve","age":18}},\n
                    {"id":"1","type":"relationship","label":"KNOWS","properties":{"since":2003},"start":{"id":"3","labels":["User"]},"end":{"id":"4","labels":["User"]}}\n
                   ]'
        }
        NOTE: the Neo4j Browser uses a slightly different format for NODES:
                {
                  "identity": 4,
                  "labels": [
                    "User"
                  ],
                  "properties": {
                    "name": "Eve",
                    "age": 18
                  }
                }
              and a substantially more different format for RELATIONSHIPS:
                {
                  "identity": 1,
                  "start": 3,
                  "end": 4,
                  "type": "KNOWS",
                  "properties": {
                    "since": 2003
                  }
                }
        :return:    A dictionary specifying the number of nodes exported,
                    the number of relationships, and the number of properties,
                    as well as a "data" field with the actual export in JSON format
        """
        cypher = '''
            CALL apoc.export.json.all(null,{useTypes:true, stream: true})
            YIELD nodes, relationships, properties, data
            RETURN nodes, relationships, properties, data
            '''
        result = self.query(cypher)  # It returns a list with a single element
        export_dict = result[0]
        # print(export_dict)

        pseudo_json = export_dict["data"]
        # Who knows why, the string returned by the APOC function isn't actual JSON! :o  Some tweaking needed to produce valid JSON...
        json = "[" + pseudo_json.replace("\n", ",\n ") + "\n]"  # The newlines \n make the JSON much more human-readable
        export_dict["data"] = json
        # print(export_dict)

        return export_dict

    def import_json_data(self, json_str: str):
        """
        Import nodes and/or relationships into the database, as directed by the given data dump in JSON form.
        Note: the id's of the nodes need to be shifted,
              because one cannot force the Neo4j internal id's to be any particular value...
              and, besides (if one is importing into an existing database), particular id's may already be taken.
        :param json_str:    A JSON string with the format specified under export_dbase_json()
        :return:            A status message with import details if successful, or an Exception if not
        """

        try:
            json_list = json.loads(json_str)  # Turn the string (representing a JSON list) into a list
        except Exception as ex:
            raise Exception(f"Incorrectly-formatted JSON string. {ex}")

        if self.debug:
            print("json_list: ", json_list)

        assert type(json_list) == list, "The JSON string does not represent the expected list"

        id_shifting = {}  # To map the Neo4j internal ID's specified in the JSON data dump
        #       into the ID's of newly-created nodes

        # Do an initial pass for correctness, to try to avoid partial imports
        for i, item in enumerate(json_list):
            # We use item.get(key_name) to handle without error situation where the key is missing
            if (item.get("type") != "node") and (item.get("type") != "relationship"):
                raise Exception(
                    f"Item in list index {i} must have a 'type' of either 'node' or 'relationship'.  Nothing imported.  Item: {item}")

            if item["type"] == "node":
                if "id" not in item:
                    raise Exception(
                        f"Item in list index {i} is marked as 'node' but it lacks an 'id'.  Nothing imported.  Item: {item}")

            elif item["type"] == "relationship":
                if "label" not in item:
                    raise Exception(
                        f"Item in list index {i} is marked as 'relationship' but lacks a 'label'.  Nothing imported.  Item: {item}")
                if "start" not in item:
                    raise Exception(
                        f"Item in list index {i} is marked as 'relationship' but lacks a 'start' value.  Nothing imported.  Item: {item}")
                if "end" not in item:
                    raise Exception(
                        f"Item in list index {i} is marked as 'relationship' but lacks a 'end' value.  Nothing imported.  Item: {item}")
                if "id" not in item["start"]:
                    raise Exception(
                        f"Item in list index {i} is marked as 'relationship' but its 'start' value lacks an 'id'.  Nothing imported.  Item: {item}")
                if "id" not in item["end"]:
                    raise Exception(
                        f"Item in list index {i} is marked as 'relationship' but its 'end' value lacks an 'id'.  Nothing imported.  Item: {item}")

        # First, process all the nodes, and in the process create the id_shifting map
        num_nodes_imported = 0
        for item in json_list:
            if item["type"] == "node":
                if self.debug:
                    print("ADDING NODE: ", item)
                    print(f'     Creating node with label `{item["labels"][0]}` and properties {item["properties"]}')
                old_id = int(item["id"])
                new_id = self.create_node_by_label_and_dict(item["labels"][0], item[
                    "properties"])  # TODO: Only the 1st label is used for now
                id_shifting[old_id] = new_id
                num_nodes_imported += 1

        if self.debug:
            print("id_shifting map:", id_shifting)

        # Then process all the relationships, linking to the correct (newly-created) nodes by using the id_shifting map
        num_rels_imported = 0
        for item in json_list:
            if item["type"] == "relationship":
                if self.debug:
                    print("ADDING RELATIONSHIP: ", item)

                rel_name = item["label"]
                rel_props = item.get(
                    "properties")  # Also works if no "properties" is present (relationships may lack it)

                start_id_original = int(item["start"]["id"])
                end_id_original = int(item["end"]["id"])

                start_id_shifted = id_shifting[start_id_original]
                end_id_shifted = id_shifting[end_id_original]
                # print(f'     Creating relationship named `{rel_name}` from node {start_id_shifted} to node {end_id_shifted},  with properties {rel_props}')

                self.link_nodes_by_ids(start_id_shifted, end_id_shifted, rel_name, rel_props)
                num_rels_imported += 1

        return f"Successful import of {num_nodes_imported} node(s) and {num_rels_imported} relationship(s)"

    ############################################################################################
    #                                                                                          #
    #                           METHODS TO SUPPORT RDF PROCEDURES                              #
    #                                                                                          #
    ############################################################################################

    def rdf_generate_uri(self,
                         dct={},
                         include_label_in_uri=True,
                         prefix='neo4j://graph.schema#',
                         add_prefixes=[],
                         sep='/',
                         uri_prop='uri') -> None:
        """
        A method that
            - on the neo4j nodes with labels equal to keys of :dict dictionary
            - sets additional label Resource (for handling in RDF)
            - sets property with name :uri_prop with value that starts with prefix followed by a string
            built by concatenating with separator :sep the list of :add_prefixes together with values of
            properties on each node that are specified in the values of the :dict (different set for each Neo4j label)
        Used for the purpose of being able to save and restore subgraphs using methods rdf_get_subgraph and
        rdf_import_subgraph_inline.
        :param dct: dictionary describing set of node properties that construct a primary key (and eventually uri) for that node
            EXAMPLE1 (simple):
                dct = {
                    'Vehicle': ['type', 'model'],
                    'Car': ['model', 'fuel']
                }
                generate_uri(dct)
                will set property uri like 'neo4j://graph.schema#car/toyota' on nodes with labels Vehicle
                (in case v.type == 'car' and v.model == 'toyota')
                and set property uri like 'neo4j://graph.schema#toyota/petrol' on nodes with labels Car
                (in case c.model == 'toyota' and v.fuel == 'petrol')
            EXAMPLE2 (properties and neighbouring properties):
                graph = CREATE (v:Vehicle{`producer`: 'Toyota'}),
                        (m:Model{`name`: 'Prius'}),
                        (v)-[:HAS_MODEL]->(m)
                dct = {
                "Vehicle": {"properties": "producer"},
                "Model": {"properties": ["name"],
                           "neighbours": [
                            {"label": "Vehicle", "relationship": "HAS_MODEL", "property": producer"}
                           ]
                         }
                }
                set URI on 'Vehicle' nodes using node's property "producer"
                    uri = 'neo4j://graph.schema#Vehicle/Toyota'
                set URI on 'Model' nodes using node's property "name" and neighbouring node's property "producer"
                    uri = 'neo4j://graph.schema#Model/Toyota/Prius'
        :param prefix: a prefix for uri
        :param add_prefixes: list of prefixes to prepend uri with (after prefix), list joined with :sep separator
        :param sep: separator for joining add_perfixes and the primary keys into uri
        :return: None
        """
        for label, config in dct.items():
            assert isinstance(label, str)
            assert any(isinstance(config, t) for t in [list, str, dict])
            where = ""
            neighbours = False
            neighbours_query = ""
            if isinstance(config, str):
                properties_ext = [config]
            elif isinstance(config, list):
                properties_ext = config
            elif isinstance(config, dict):
                if 'properties' in config.keys():
                    if isinstance(config['properties'], str):
                        properties_ext = [config['properties']]
                    elif isinstance(config['properties'], list):
                        properties_ext = config['properties']
                if 'neighbours' in config.keys():
                    assert isinstance(config['neighbours'], list), \
                        f"neighbours should be of type LIST [{{}}[, {{}}]] not {type(config['neighbours'])}"
                    for i, neighbour in enumerate(config['neighbours']):
                        if isinstance(neighbour, list): #if a list converting it to a dict as per req.
                            assert len(neighbour) == 3, \
                                f"each neighbour should be of length 3: [<label>, <relationship>, <property>] got: {neighbour}"
                            neighbour = {'label': neighbour[0], 'relationship': neighbour[1], 'property': neighbour[2]}
                            config['neighbours'][i] = neighbour
                        assert isinstance(neighbour, dict), \
                            f"each neighbour should be of type DICT not {type(neighbour)}"
                        for key in ['label', 'relationship', 'property']:
                            assert key in neighbour.keys(), f"{key} not found in {neighbour}"
                    neighbours = True
                    neighbours_query = """
                                        WITH *
                                        UNWIND apoc.coll.zip(range(0,size($neighbours)-1), $neighbours) as pair
                                        WITH *, pair[0] as ind, pair[1] as neighbour
                                        CALL apoc.path.expand(x, neighbour['relationship'], neighbour['label'], 1, 1)
                                        YIELD path
                                        WITH x, ind, nodes(path) as ind_neighbours
                                        UNWIND ind_neighbours as nbr
                                        WITH DISTINCT x, ind, nbr
                                        WHERE x<>nbr 
                                        WITH * 
                                        ORDER BY x, ind, id(nbr) 
                                        WITH x, ind, collect(nbr) as coll 
                                        WITH x, ind, apoc.map.mergeList(coll) as nbr
                                        WITH x, collect({index: ind, map: nbr}) as nbrs"""
                if 'where' in config.keys():
                    where = config['where']
            else:
                properties_ext = []

            cypher = f"""
            MATCH (x:`{label}`)
            {where}
            {neighbours_query}
            SET x:Resource            
            SET
            x.
            `{uri_prop}` = apoc.text.regreplace(
                $prefix + apoc.text.join($add_prefixes + $opt_label + 
{"[nbr in nbrs | nbr['map'][$neighbours[nbr['index']]['property']]] +" if neighbours else ""} 
[prop in $properties | x[prop]], $sep)
            ,
                '\\s'
            ,
                '%20'
            )  // for the sake of uri spaces are replaced with %20             
            """
            cypher_dict = {
                'prefix': prefix,
                'add_prefixes': add_prefixes,
                'sep': sep,
                'opt_label': ([label] if include_label_in_uri else []),
                'properties': properties_ext
            }
            if neighbours:
                cypher_dict.update({
                    'neighbours': config['neighbours']
                })

            if self.debug:
                print(f"""
                query: {cypher}
                parameters: {cypher_dict}
                """)
            self.query(cypher, cypher_dict)

    def rdf_get_subgraph(self, cypher: str, cypher_dict={}, format="Turtle-star") -> str:
        """
        A method that returns an RDF serialization of a subgraph specified by :cypher query
        :param cypher: cypher query to return a subgraph
        :param cypher_dict: parameters required for the cypher query
        :param format: RDF format in which to serialize output
        :return: str - RDF serialization of subgraph
        """
        url = self.rdf_host + "neo4j/cypher"
        j = ({'cypher': cypher, 'format': format, 'cypherParams': cypher_dict})
        response = requests.post(
            url=url,
            json=j,
            auth=self.credentials)
        # TODO: switch to detached HTTP endpoint when code from neo4j is available
        # see https://community.neo4j.com/t/export-procedure-that-returns-serialized-rdf/38781/2
        return response.text

    def rdf_import_fetch(self, url: str, format="Turtle-star"):
        cypher = "CALL n10s.rdf.import.fetch ($url, $format) YIELD terminationStatus, triplesLoaded, triplesParsed, " \
                 "namespaces, extraInfo, callParams"
        cypher_dict = {'url': url, 'format': format}
        if self.debug:
            print(f"""
                query: {cypher}
                parameters: {cypher_dict}
                """)
        return self.query(cypher, cypher_dict)

    def rdf_import_subgraph_inline(self, rdf: str, format="Turtle-star"):
        """
        A method that creates/merges appropriate nodes in Neo4j as specified in the provided :rdf string
        The nodes will be MERGEd by 'uri' property
        :param rdf: RDF serialization of Neo4j nodes and relationships
        :param format: RDF serialization format
        :return: returns a dictionary with keys triplesParsed, triplesLoaded as a summary of the operation
        """
        assert self.rdf, "rdf option is not enabled at init of NeoInterface class"
        if not self.autoconnect:
            self.rdf_setup_connection()
        cypher = """
        CALL n10s.rdf.import.inline($rdf, $format) 
        YIELD triplesParsed, triplesLoaded, extraInfo 
        RETURN *
        """
        # cypher_dict = {'rdf':rdf.encode('utf-8').decode('utf-8'), 'format': format}
        cypher_dict = {'rdf': rdf, 'format': format}
        if self.debug:
            print(f"""
            query: {cypher}
            parameters: {cypher_dict}
            """)
        res = self.query(cypher, cypher_dict)
        self._rdf_import_subgraph_cleanup()
        if len(res) > 0:
            return res[0]
        else:
            return {'triplesParsed': 0, 'triplesLoaded': 0, 'extraInfo': ''}

    def _rdf_import_subgraph_cleanup(self):
        # in case labels with spaces where serialized new labels with spaces being replaced with %20 could have been created
        # this helper function is supposed to revert the change
        cypher = """
            UNWIND $labels as label
            CALL apoc.refactor.rename.label(label, apoc.text.regreplace(label, '%20', ' '))
            YIELD batches, failedBatches, total, failedOperations 
            RETURN batches, failedBatches, total, failedOperations
        """
        cypher_dict = {'labels': [label for label in self.get_labels() if "%20" in label]}
        if self.debug:
            print(f"""
            query: {cypher}
            parameters: {cypher_dict}
            """)
        self.query(cypher, cypher_dict)

        # in case properties with spaces where serialized new properties with spaces being replaced with %20 could have been created
        # this helper function is supposed to revert the change
        cypher2 = """
        CALL db.schema.nodeTypeProperties() YIELD nodeLabels, propertyName
        WHERE propertyName contains "%20"
        CALL apoc.cypher.doIt(
            'MATCH (node:`' + apoc.text.join(nodeLabels, '`:`') + '`) ' + 
            'WHERE "' + propertyName + '" in keys(node)' + 
            'SET node.`' + apoc.text.replace(propertyName, '%20', ' ') + '` = node.`' + propertyName + '`' + 
            'REMOVE node.`' + propertyName + '`'        
            ,
            {}
        ) YIELD value
        RETURN value['node']
        """
        cypher_dict2 = {}
        if self.debug:
            print(f"""
            query: {cypher2}
            parameters: {cypher_dict2}
            """)
        self.query(cypher2, cypher_dict2)

    def rdf_get_graph_onto(self):
        """
        A method that returns an ontology autogenerated from existing nodes in Neo4j (provided by n10s(neosemantics) library
        :return: str - serialized ontology
        """
        assert self.rdf, "rdf option is not enabled at init of NeoInterface class"
        url = self.rdf_host + "neo4j/onto"
        response = requests.get(
            url=url,
            auth=self.credentials)
        return response.text

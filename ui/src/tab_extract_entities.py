from neointerface import NeoInterface
import panel as pn
import json

class tab_extract_entities(pn.Column):
    def __init__(self, interface:NeoInterface) -> None:
        self.interface = interface        
        self.extract_entities_from_class = pn.widgets.Select(name='From Class', options=[])
        self.extract_entities_from_prop = pn.widgets.MultiSelect(name='From Property', options=[])
        self.extract_entities_to_class = pn.widgets.TextInput(name='To Class')
        self.extract_entities_to_prop = pn.widgets.TextInput(name='Rename Properties', value = str({"source": "target"}))
        self.extract_entities_button_extract = pn.widgets.Button(name='Extract', button_type='primary')
        self.extract_entities_rel_type = pn.widgets.TextInput(name='Relationship type', value='FROM')        
        self.extract_entities_rel_dir = pn.widgets.Select(name='Relationship direction', options=['<', '>'], value='<')
        self.extract_entities_button_extract.on_click(self.extract_entities_on_button_clicked)
        self.extract_entities_status = pn.indicators.BooleanStatus(value=False, color='success')

        self.watcher_extract_entities_from_class = self.extract_entities_from_class.param.watch(
            self.extract_entities_on_updated_from_class, "value")
        self.watcher_extract_entities_from_prop = self.extract_entities_from_prop.param.watch(
            self.extract_entities_on_updated_from_property, "value")

        super().__init__(
            pn.Row(self.extract_entities_from_class, self.extract_entities_from_prop),
            pn.Row(self.extract_entities_to_class, self.extract_entities_to_prop),
            self.extract_entities_rel_type,
            self.extract_entities_rel_dir,
            self.extract_entities_button_extract,
            self.extract_entities_status,
            background='WhiteSmoke'
        )

    def extract_entities_on_updated_from_class(self, event):
        with open("ui/cypher/get_nodeType_properties_by_label.cypher", "r") as f:
            q = f.read()
        res = self.interface.query(
            q,
            {"label": event.obj.value}
        )
        self.extract_entities_from_prop.options = [r["propertyName"] for r in res]

    def extract_entities_on_updated_from_property(self, event):        
        if event.obj.value:
            self.extract_entities_to_class.value = event.obj.value[0]
            self.extract_entities_status.value = False
        
    def extract_entities_on_button_clicked(self, event):
        self.extract_entities_status.value = False    
        if self.extract_entities_from_class.value and self.extract_entities_from_prop.value and self.extract_entities_to_class.value:
            try:
                rename_dict = json.loads(self.extract_entities_to_prop.value)
            except:
                rename_dict = {}
            property_mapping = {
                k: (rename_dict[k] if k in rename_dict.keys() else k) for k in self.extract_entities_from_prop.value
            }        
            self.interface.extract_entities(
                label=self.extract_entities_from_class.value,
                target_label=self.extract_entities_to_class.value,            
                property_mapping=property_mapping,
                relationship=self.extract_entities_rel_type.value,
                direction=self.extract_entities_rel_dir.value,
            )
            self.extract_entities_status.value = True    
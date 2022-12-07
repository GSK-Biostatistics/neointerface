from neointerface import NeoInterface
import panel as pn 
import pandas as pd

class tab_link_entities(pn.Column):
    def __init__(self, interface:NeoInterface) -> None:
        self.interface = interface
        self.link_entities_left_class = pn.widgets.Select(name="Left Node", options=[])
        self.link_entities_right_class = pn.widgets.Select(name="Right Node", options=[])
        self.link_entities_rel_type = pn.widgets.TextInput(name="Relationship Type", value="_default_")
        self.link_entities_cond_left_rel = pn.widgets.Select(options=[])
        self.link_entities_cond_via_node = pn.widgets.Select(options=[])
        self.link_entities_cond_right_rel = pn.widgets.Select(options=[])        
        self.link_entities_condition = pn.Row(        
                "## Condition to link: ",
                f"## (:",
                "",
                "## )-[:",
                self.link_entities_cond_left_rel,
                f"## ]-(:",
                self.link_entities_cond_via_node,
                f"## )-[:",
                self.link_entities_cond_right_rel,
                f"## ]-(:",
                "",
                "## )"
            )
        self.link_entities_button_link = pn.widgets.Button(name='Create Relationships', button_type='primary')
        self.link_entities_status = pn.indicators.BooleanStatus(value=False, color='success')

        self.watcher_link_entities_left_class = self.link_entities_left_class.param.watch(
            self.link_entities_on_changed_any_class, "value")
        self.watcher_link_entities_right_class = self.link_entities_right_class.param.watch(
            self.link_entities_on_changed_any_class, "value")
        self.link_entities_button_link.on_click(self.link_entities_on_button_clicked)
    
        super().__init__(
            pn.Row(
                self.link_entities_left_class,
                self.link_entities_right_class,        
            ),    
            self.link_entities_rel_type,    
            self.link_entities_condition,
            self.link_entities_button_link,
            self.link_entities_status,  
        )

    def link_entities_on_changed_any_class(self, event):  
        self.link_entities_status.value = False
        self.link_entities_condition[2] = f"## {self.link_entities_left_class.value}"
        self.link_entities_condition[-2] = f"## {self.link_entities_right_class.value}"
        if self.link_entities_left_class.value and self.link_entities_right_class.value:
            with open("ui/cypher/get_link_entities_rels.cypher", "r") as f:
                q = f.read()
            res = self.interface.query(
                q
                ,
                {
                    "left_label": self.link_entities_left_class.value,
                    "right_label": self.link_entities_right_class.value
                }
            )
            if res:
                res_df = pd.DataFrame(res)
                self.link_entities_cond_left_rel.options = list(f"{x}>" for x in set(res_df['r1']))
                self.link_entities_cond_via_node.options = list(set(res_df['lbl']))
                self.link_entities_cond_right_rel.options = list(f"<{x}" for x in set(res_df['r2']))
            else:
                self.link_entities_cond_left_rel.options = []
                self.link_entities_cond_via_node.options = []
                self.link_entities_cond_right_rel.options = []

    def link_entities_on_button_clicked(self, event):
        self.link_entities_status.value = False    
        self.interface.debug=True
        self.interface.link_entities(
            left_class=self.link_entities_left_class.value,
            right_class=self.link_entities_right_class.value,
            relationship=self.link_entities_rel_type.value,                    
            cond_left_rel=self.link_entities_cond_left_rel.value,
            cond_via_node=self.link_entities_cond_via_node.value,
            cond_right_rel=self.link_entities_cond_right_rel.value,
        )
        self.link_entities_status.value = True


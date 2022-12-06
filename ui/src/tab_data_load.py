from neointerface import NeoInterface
import panel as pn
import pandas as pd
import io

class tab_data_load(pn.Column):
    def __init__(self, interface:NeoInterface) -> None:
        self.interface = interface
        self.data_load_file_input_title = "## Load csv"
        self.data_load_file_input = pn.widgets.FileInput(accept='.csv')
        self.data_load_df_preview_title = "## Preview"
        self.data_load_df_load_label = pn.widgets.TextInput(name='Neo4j Label', placeholder='Enter Label for the file data in Neo4j')
        self.data_load_df0 = pd.DataFrame([], columns=["test"])
        self.data_load_tbr0 = pn.widgets.Tabulator(self.data_load_df0)
        self.data_load_button_load = pn.widgets.Button(name='Load to Neo4j', button_type='primary')
        self.data_load_status = pn.indicators.BooleanStatus(value=False, color='success')

        self.watcher_file_input = self.data_load_file_input.param.watch(self.file_input_update_df, "value")
        self.data_load_button_load.on_click(self.button_load_to_neo4j_clicked)

        super().__init__(
            self.data_load_file_input_title,
            pn.Row(self.data_load_file_input, background='WhiteSmoke'),
            self.data_load_df_load_label,
            self.data_load_button_load,
            self.data_load_status,
            self.data_load_df_preview_title,            
            self.data_load_tbr0,                        
            background='WhiteSmoke'
        )
    
    def file_input_update_df(self, event):
        file_input_value = event.obj.value
        if file_input_value is not None:    
            out = io.BytesIO()
            event.obj.save(out)
            out.seek(0)
            self.data_load_df_load_label.value = ".".join(event.obj.filename.split(".")[:-1]).upper()
            self.data_load_tbr0.value = pd.read_csv(out)        

    def button_load_to_neo4j_clicked(self, event):    
        self.data_load_status.value = False
        df = pd.DataFrame(
            self.interface.load_df(
                df = self.data_load_tbr0.value, 
                label = self.data_load_df_load_label.value
            )
        )    
        if not df.empty:
            self.data_load_status.value = True    
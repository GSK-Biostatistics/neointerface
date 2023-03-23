from neointerface import NeoInterface
import panel as pn

class tab_clear_db(pn.Column):
    def __init__(self, interface: NeoInterface) -> None:        
        self.interface = interface
        self.clear_db_button_clear = pn.widgets.Button(name='Clear Database', button_type='primary')
        self.clear_db_button_clear.on_click(self.clear_db_button_clicked)        
        super().__init__(self.clear_db_button_clear)

    def clear_db_button_clicked(self, event):    
        self.interface.clean_slate()    
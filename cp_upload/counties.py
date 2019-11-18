class Counties:
    def __init__(self):
        print("Construction Counties")

    def is_bay_area(self, city):
		if (city == "san francisco" or
	    self.is_san_mateo_county(city) or
		  self.is_santa_clara_county(city) or
		  self.is_marin_county(city) or
		  self.is_napa_county(city) or
		  self.is_solano_county(city) or
		  self.is_sonoma_county(city) or
		  self.is_alameda_county(city) or
		  self.is_conta_costa_county(city)):
		    return True
		else:
			return False

    def is_sonoma_county(self, city ):
	    if (city == "cloverdale" or
	      city == "cotati" or
	      city == "healdsburg" or
	      city == "petaluma" or
	      city == "rohnert park" or
	      city == "santa rosa" or
	      city == "sebastopol" or
	      city == "sonoma" or
	      city == "windsor"):
	        return True
	    else:
	        return False

    def is_solano_county(self, city ):
	    if (city == "benicia" or
	      city == "birds landing" or
	      city == "dixon" or
	      city == "elmira" or
	      city == "fairfield" or
	      city == "rio vista" or
	      city == "suisun city" or
	      city == "vacaville" or
	      city == "vallejo"):
	        return True
	    else:
	        return False

    def is_napa_county(self, city ):
	    if (city == "american canyon" or
	      city == "angwin" or
	      city == "calistoga" or
	      city == "deer park" or
	      city == "napa" or
	      city == "oakville" or
	      city == "pope valley" or
	      city == "rutherford" or
	      city == "saint helena" or
	      city == "yountville"):
	        return True
	    else:
	        return False

    def is_marin_county(self, city ):
	    if (city == "belvedere" or
	      city == "corte madera" or
	      city == "fairfax" or
	      city == "larkspur" or
	      city == "mill valley" or
	      city == "novato" or
	      city == "ross" or
	      city == "san anselmo" or
	      city == "san rafael" or
	      city == "sausalito" or
	      city == "tiburon"):
	        return True
	    else:
	        return False

    def is_conta_costa_county(self, city ):
	    if (city == "concord" or
	      city == "antioch" or
	      city == "clayton" or
	      city == "danville" or
	      city == "el cerrito" or
	      city == "hercules" or
	      city == "martinez" or
	      city == "moraga" or
	      city == "oakley" or
	      city == "pinole" or
	      city == "pittsburg" or
	      city == "pleasant hill" or
	      city == "richmond" or
	      city == "san pablo" or
	      city == "san ramon" or
	      city == "orinda" or
	      city == "lafayette" or
	      city == "moraga" or
	      city == "walnut creek"):
	        return True
	    else:
	        return False

    def is_alameda_county(self, city ):
	    if (city == "oakland" or
	      city == "fremont" or
	      city == "alameda" or
	      city == "albany" or
	      city == "livermore" or
	      city == "berkeley" or
	      city == "newark" or 
	      city == "dublin" or
	      city == "emeryville" or
	      city == "piedmont" or
	      city == "pleasanton" or
	      city == "hayward" or
	      city == "san leandro" or
	      city == "san lorenzo"):
	        return True
	    else:
	        return False

    def is_santa_clara_county(self, city ):
	    if (city == "milpitas" or
	      city == "san jose" or
	      city == "campbell" or
	      city == "cupertino" or
	      city == "gilroy" or
	      city == "los altos" or
	      city == "los gatos" or
	      city == "morgan hill" or
	      city == "mountain view" or
	      city == "saratoga" or
	      city == "sunnyvale" or
	      city == "santa clara" or
	      city == "palo alto"):
	        return True
	    else:
	        return False

    def is_san_mateo_county(self, city ):
	    if (city == "atherton" or
	      city == "belmont" or
	      city == "brisbane" or
	      city == "burlingame" or
	      city == "colma" or
	      city == "daly city" or
	      city == "east palo alto" or
	      city == "foster city" or
	      city == "half moon bay" or
	      city == "hillsborough" or
	      city == "menlo park" or
	      city == "milbrae" or
	      city == "pacifica" or
	      city == "portola valley" or
	      city == "redwood city" or
	      city == "san bruno" or
	      city == "san carlos" or
	      city == "san mateo" or
	      city == "south san francisco" or
	      city == "woodside"):
	        return True
	    else:
	        return False

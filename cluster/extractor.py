from KINCluster.core.extractor import Extractor, extractable 

class Extractor(Extractor):

    def __get_noun(self, items) -> List[str]:
        pass

    @extractable
    def related(self, iid: itemID) -> List[str]:
        pass

    @extractable
    def special(self, iid: itemID) -> List[str]:
        pass
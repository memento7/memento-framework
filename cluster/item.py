from KINCluster.core.item import Item

class myItem(Item):
    def __repr__(self):
        quotes = " ".join(self._element['quotes'])
        return quotes + " " + " ".join(map(str, map(lambda x: self._element[x], ['title', 'content', 'keyword'])))

    def __str__(self):
        quotes = " ".join(self._element['quotes'])
        return quotes + " " + " ".join(map(str, map(lambda x: self._element[x], ['title', 'content', 'keyword'])))

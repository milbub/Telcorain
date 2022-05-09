import configparser
import codecs
from os.path import exists


class LinksetsManager:
    def __init__(self, links: dict):
        self.links = links
        self.sets_path = './linksets.ini'

        self.linksets = configparser.ConfigParser()
        self.set_names = []

        if exists(self.sets_path):
            self.linksets.read(self.sets_path, encoding='utf-8')
            self.set_names = self.linksets.sections()

        # ////// SQLite -> ini file synchronization \\\\\\

        # check listed links in link sets and remove old/deleted/invalid links
        if len(self.linksets['DEFAULT']) > 0:
            links_for_del = []

            for link_id in self.linksets['DEFAULT']:
                if int(link_id) not in self.links:
                    links_for_del.append(link_id)

            if len(links_for_del) > 0:
                for link_id in links_for_del:
                    self.linksets['DEFAULT'].pop(link_id)

                for link_set in self.set_names:
                    for link_id in links_for_del:
                        self.linksets[link_set].pop(link_id)

        # add new/missing links into linksets default list
        for link_id in self.links:
            if str(link_id) not in self.linksets['DEFAULT']:
                self.linksets['DEFAULT'][str(link_id)] = '3'

        # save changes into ini
        self._save()

    def create_set(self, name: str):
        self.linksets[name] = {}
        self.set_names.append(name)

        for link_id in self.linksets['DEFAULT']:
            self.linksets[name][link_id] = '0'

        self._save()

    def copy_set(self, origin_name: str, new_name: str):
        self.linksets[new_name] = {}
        self.set_names.append(new_name)

        for link_id in self.linksets[origin_name]:
            self.linksets[new_name][link_id] = self.linksets[origin_name][link_id]

        self._save()

    def delete_set(self, name: str):
        self.linksets.remove_section(name)
        self._save()

    def insert_link(self, set_name: str, link_id: int, channels: int):
        self.linksets[set_name][str(link_id)] = str(channels)
        self._save()

    def delete_link(self, set_name: str, link_id: int):
        self.linksets[set_name].pop(str(link_id), None)
        self._save()

    def _save(self):
        with codecs.open(self.sets_path, 'w', 'utf-8') as setsfile:
            self.linksets.write(setsfile)

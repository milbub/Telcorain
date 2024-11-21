import configparser
import codecs
from os.path import exists

from handlers.logging_handler import logger

class LinksetsHandler:
    def __init__(self, links: dict):
        self.links = links
        self.sets_path = './linksets.ini'

        self.linksets = configparser.ConfigParser()
        self.sections = []

        if exists(self.sets_path):
            self.linksets.read(self.sets_path, encoding='utf-8')
            self.sections = self.linksets.sections()

        # ////// SQL DB -> ini file synchronization \\\\\\

        # check listed links in link sets and remove old/deleted/invalid links
        if len(self.linksets['DEFAULT']) > 0:
            links_for_del = []

            for link_id in self.linksets['DEFAULT']:
                if int(link_id) not in self.links:
                    links_for_del.append(link_id)

            if len(links_for_del) > 0:
                for link_id in links_for_del:
                    self.linksets['DEFAULT'].pop(link_id)

                for link_set in self.sections:
                    for link_id in links_for_del:
                        try:
                            self.linksets[link_set].pop(link_id)
                        except KeyError:
                            logger.warning(f"Deleted link ID {link_id} not found in link set {link_set}.")

        # add new/missing links into linksets default list
        for link_id in self.links:
            if str(link_id) not in self.linksets['DEFAULT']:
                self.linksets['DEFAULT'][str(link_id)] = '3'

        # save changes into ini
        self.save()

    def create_set(self, name: str):
        self.linksets[name] = {}
        self.sections.append(name)

        for link_id in self.linksets['DEFAULT']:
            self.linksets[name][link_id] = '0'

        self.save()

    def copy_set(self, origin_name: str, new_name: str):
        self.linksets[new_name] = {}
        self.sections.append(new_name)

        for link_id in self.linksets[origin_name]:
            if self.linksets[origin_name][link_id] != 3:
                self.linksets[new_name][link_id] = self.linksets[origin_name][link_id]

        self.save()

    def delete_set(self, name: str):
        self.linksets.remove_section(name)
        self.save()

    def modify_link(self, set_name: str, link_id: int, channels: int):
        self.linksets[set_name][str(link_id)] = str(channels)

    def delete_link(self, set_name: str, link_id: int):
        self.linksets.remove_option(set_name, str(link_id))

    def save(self):
        with codecs.open(self.sets_path, 'w', 'utf-8') as setsfile:
            self.linksets.write(setsfile)

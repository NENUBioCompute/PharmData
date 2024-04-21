import traceback
from PharmDataProject.Utilities.NetDownloads.HttpDownloader import HTTP
import configparser




class Wikipathway_Downloader():

    def start(self,cfgfile):
        config = configparser.ConfigParser()
        config.read(cfgfile, encoding="utf-8")
        try:
            HTTP.download(config.get('wikipathway', 'source_url_1'),
                          config.get('wikipathway', 'data_path_1').rstrip(config.get('wikipathway', 'data_path_1').split('/')[-1]),
                          config.get('wikipathway', 'data_path_1').split('/')[-1].split('.')[0])
        except Exception:
            try:
                HTTP.download(config.get('wikipathway', 'source_url_1'),
                              config.get('wikipathway', 'data_path_1').rstrip(config.get('wikipathway', 'data_path_1').split('/')[-1]),
                              config.get('wikipathway', 'data_path_1').split('/')[-1].split('.')[0])
            except Exception:
                traceback.print_exc()

    def pause(self):
        pass

    def stop(self):
        pass

    def get_status(self):
        pass


if __name__ == '__main__':
    Wikipathway_Downloader().start('../conf/drugkb.config')


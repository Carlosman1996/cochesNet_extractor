class CochesNetPage:
    headers = {
        'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:107.0) Gecko/20100101 Firefox/107.0',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip,deflate,br',
        'Referer': 'https://www.coches.net/segunda-mano/'
    }
    base_url = 'http://www.coches.net/segunda-mano'

    @staticmethod
    def get_url():
        return CochesNetPage.base_url

    @staticmethod
    def get_headers():
        return CochesNetPage.headers

    @staticmethod
    def get_url_filter_most_recent(page: int):
        return CochesNetPage.base_url + '/?pg=' + str(page)

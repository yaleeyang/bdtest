from http.server import BaseHTTPRequestHandler, HTTPServer
from exchange.test.BirthdayService import BirthdayResiveService

port = 9874

# HTTPRequestHandler class
class BirthdayManagerHandler(BaseHTTPRequestHandler):

    def _set_headers(self):
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

    # GET
    def do_POST(self):
        self._set_headers()
        self.data_string = self.rfile.read(int(self.headers['Content-Length']))
        val = self.headers['Content-Length']
        self.send_response(200)
        self.end_headers()
        # data = simplejson.loads(self.data_string)
        # print(data['test'])
        path = str(self.path)
        if self.path.startswith('/bd/producer'):
            if '?' in path:
                key = path.split('=')[1]
                result = BirthdayResiveService.decryptProducerData(self,self.data_string,key)
                self.wfile.write(bytes(result, "utf8"))
        return

    def end_headers(self):
        self.send_header('Access-Control-Allow-Origin', '*')
        BaseHTTPRequestHandler.end_headers(self)

def run():
    print('starting server...')

    # Server settings
    # Choose port 8080, for port 80, which is normally used for a http server, you need root access
    server_address = ('0.0.0.0', port)
    httpd = HTTPServer(server_address, BirthdayManagerHandler)
    print('running server...{}'.format(port))
    httpd.serve_forever()


run()




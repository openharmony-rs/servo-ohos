import time


def main(request, response):
    delay = float(request.GET.first(b"ms", 500))
    time.sleep(delay / 1E3)

    # 200 OK with a JS MIME type, but a body that fails to parse. The module is
    # fetched successfully and only fails at parse time, so it reaches the
    # module graph's parse-error handling as a "straggler" completion.
    return [(b"Content-type", b"text/javascript")], u"export let x = ;"

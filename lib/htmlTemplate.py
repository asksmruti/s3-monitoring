import re
from urllib.parse import urlparse


def translate_path(val):
    try:
        if re.match(r"^s3.*://", val):
            s3url = "https://s3.console.aws.amazon.com/s3/buckets/"
            bucket_parser = urlparse(val, allow_fragments=False)
            bucket, prefix = bucket_parser.netloc, bucket_parser.path.lstrip('/')
            bucket_name = "https://s3.console.aws.amazon.com/s3/buckets/{}/{}".format(bucket, prefix)
            ref = "<a href ='" + bucket_name + "', target = '_blank'>" + val + "</a>"
            return ref
        else:
            return val
    except ValueError:
        unknown_err = 'attribute not found'
        return unknown_err


def generate(dict):
    html = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Datalaka - Monitoring</title>
</head>
<body>
<style>

table.blueTable {
  border: 1px solid #1C6EA4;
  background-color: #EEEEEE;
  width: 100%;
  text-align: left;
  border-collapse: collapse;
}
table.blueTable td, table.blueTable th {
  border: 1px solid #AAAAAA;
  padding: 3px 2px;
}
table.blueTable tbody td {
  font-size: 15px;
}
table.blueTable tr:nth-child(even) {
  background: #D0E4F5;
}
table.blueTable thead {
  background: #1C6EA4;
  background: -moz-linear-gradient(top, #5592bb 0%, #327cad 66%, #1C6EA4 100%);
  background: -webkit-linear-gradient(top, #5592bb 0%, #327cad 66%, #1C6EA4 100%);
  background: linear-gradient(to bottom, #5592bb 0%, #327cad 66%, #1C6EA4 100%);
  border-bottom: 2px solid #444444;
}
table.blueTable thead th {
  font-size: 17px;
  font-weight: bold;
  color: #ECF0F1; 
  border-left: 2px solid #D0E4F5;
}
table.blueTable thead th:first-child {
  border-left: none;
}
</style>
<table class="blueTable">
<thead>
<!-- Set headers -->
"""
    for key, val in dict.items():
        html += "\t<th>{}</th>\n".format(key)
    html += """
    </tr>
</thead>
"""
    for index, obj in enumerate(dict['Database']):
        html += '\t'
        html += '<tr>'
        for key, val in dict.items():
            # print(val[index]
            ret = translate_path(str(val[index]))
            html += "\t<td>{}</td>".format(ret)
            html += '\n\t'
        html += "</tr>"
        html += '\n'

    html += "</tr> \
\n</table> \
\n</body> \
\n</html>"
    return html

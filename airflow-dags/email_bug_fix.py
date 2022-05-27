



def history_files_warning_email(environment=None, email_typ=None, history_files_list=None, dag_name="test_1", product="promospots-ads", assets_tag="ADS:TRANSACTION-LOAD"):
    email_subject = """%s %s for %s %s %s""" %("environment","email_type", "product", "assets_tag", "dag_name")
    email_body = """<h2><p style='color:#FF0000'>***This email has been generated automatically - DO NOT REPLY Directly ***</p></h2>"""
    email_body += """<strong>Warning</strong>: Historical APN manifest files were received today containing data prior to yesterday</br></br> """
    email_body += """<strong>Filenames:</strong>"""
    email_body += """<ul>"""
    for filename in history_files_list:
        email_body += """<li> %s </li>""" % filename
    email_body += """</ul>"""
    email_body += """<strong>Please validate Historical file(s) and if needed, requeset operations to run " \
                  "adhoc DAG for historical file(s). </strong>"""

    send_warning_email(recipent_email="", subject=email_subject, body=email_body)
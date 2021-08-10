import base64
        
jstring = input('what string do you want to parse? ')
jstring_bytes = jstring.encode('ascii')
decoded_jstring = base64.urlsafe_b64decode(jstring_bytes)

print(decoded_jstring)

#base64_url = 'aHR0cHM6Ly92ZW50dXJlLmNvbS9kb21haW5z'
#base64_bytes = base64_url.encode('ascii')
#url_bytes = base64.b64decode(base64_bytes)
#url = url_bytes.decode('ascii')
#domain = url.split('//')[1]

#print(domain)


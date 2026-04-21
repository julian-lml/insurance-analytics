import imaplib
mail = imaplib.IMAP4_SSL("imap.ligagent.com", 993)  # replace with real host + port
mail.login("laylahealth@ligagent.com", "Health123")          # replace with real credentials
print("Connected:", mail.state)
mail.logout()
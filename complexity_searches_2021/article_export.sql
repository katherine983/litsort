SELECT * FROM Articles
JOIN Journals ON Articles.journal_id = Journals.id
JOIN First_Authors ON Articles.auth_id = First_Authors.id
-- First digit of the ZIP codes of each state, territory and military region (USPS zones)
select zip_first_digit, unnest(states) as state
from (values
    ('0', ['CT','MA','ME','NH','NJ','PR','RI','VT','VI','AE']),
    ('1', ['DE','NY','PA']),
    ('2', ['DC','MD','NC','SC','VA','WV']),
    ('3', ['AL','FL','GA','MS','TN','AA']),
    ('4', ['IN','KY','MI','OH']),
    ('5', ['IA','MN','MT','ND','SD','WI']),
    ('6', ['IL','KS','MO','NE']),
    ('7', ['AR','LA','OK','TX']),
    ('8', ['AZ','CO','ID','NM','NV','UT','WY']),
    ('9', ['AK','CA','HI','OR','WA','AS','GU','MP','PW','FM','MH','AP'])
) as t(zip_first_digit, states)

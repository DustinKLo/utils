
def pull_jql_data(jql_string, token):
    url = 'https://mixpanel.com/api/2.0/jql'
    token = token
    jql_data = { 'script': jql_string }
    jql_data = requests.get(url, auth=(token, None), data=jql_data)

    return json.loads(jql_data.content)



if __name__ == '__main__':
	import os

	jql = '''
	  function main() {
        return Events({
          from_date: '2018-10-09',
          to_date:   '2018-10-10'
        })
        .groupBy(["name"], mixpanel.reducer.count());
      }
	'''
	token = os.environ['MIXPANEL_TOKEN']
	
	mixpanel_data = pull_jql_data(jql, token)
	print(mixpanel_data)

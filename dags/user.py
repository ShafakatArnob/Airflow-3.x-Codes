from airflow.sdk import asset, Asset, Context


@asset(
    name="user",
    schedule="@daily",
    uri="https://randomuser.me/api/"
)
def user(self) -> dict[str]:
    import requests

    r = requests.get(self.uri)
    return r.json()  # as it returns a value, it will be stored in the XCom of the task instance (ti)


@asset(
    name="user_location",
    schedule=user  # as soon as 'user' asset materializes, this 'user_location' asset will materialize
)
def user_location(user: Asset, context: Context) -> dict[str]:
    # the 'user' asset and the asset's 'context' are accessed by defining the type as Asset & Context
    # the Context holds the asset's xcom value
    user_data = context['ti'].xcom_pull(
        dag_id=user.name,  # behind the scenes, the asset is a dag with a single task
        task_ids=user.name,  # The name we set as asset name becomes the dag_id & task_id for that asset
        include_prior_dates=True  # to fetch the latest XCom returned by the 'user' asset, if that asset has run before
    )
    return user_data['results'][0]['location']
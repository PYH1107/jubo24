def individual_serializer(todo) -> dict:
    return {
        "id":str(todo["_id"]),
        "name": todo["name"],
        "description": todo["description"],
        "complete": todo["complete"]
    }

def list_serial(todos) -> list:
    return{individual_serializer(todo) for todo in todos}
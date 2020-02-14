def add(data):
    mysum = 0
    for num in data:
        mysum += int(num)
    return mysum

def infinite(data):
    return "$FUNC{tester.infinite,1,2}"


def convert(data):
    num = int(data['start_val'])
    return {data['start_name']: str(num * 2)}

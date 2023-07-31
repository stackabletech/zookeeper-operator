#!/usr/bin/env python3
import requests


def check_received_events():
    response = requests.post(
        'http://zookeeper-vector-aggregator:8686/graphql',
        json={
            'query': """
                {
                    transforms(first:100) {
                        nodes {
                            componentId
                            metrics {
                                receivedEventsTotal {
                                    receivedEventsTotal
                                }
                            }
                        }
                    }
                }
            """
        }
    )

    assert response.status_code == 200, \
        'Cannot access the API of the vector aggregator.'

    result = response.json()

    transforms = result['data']['transforms']['nodes']
    for transform in transforms:
        receivedEvents = transform['metrics']['receivedEventsTotal']
        componentId = transform['componentId']

        if componentId == 'filteredInvalidEvents':
            assert receivedEvents is None or \
                receivedEvents['receivedEventsTotal'] == 0, \
                'Invalid log events were processed.'
        else:
            assert receivedEvents is not None and \
                receivedEvents['receivedEventsTotal'] > 0, \
                f'No events were processed in "{componentId}".'


if __name__ == '__main__':
    check_received_events()
    print('Test successful!')

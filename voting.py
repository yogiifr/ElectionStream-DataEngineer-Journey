import psycopg2
from datetime import datetime
import simplejson as json
import confluent_kafka import Consumer, KafkaException, KafkaError, SerializingProducer

conf = {
    'boostrap.servers': 'localhost:9092'
}

consumer = Consumer(conf | {
    'group.id': 'voting-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
})

producer = SerializingProducer()

if __name__ == "__main__":
    conn = psycopg2.connect("host=localhost dbname=voting user=postgres password=postgres")
    cur = conn.cursor()

    candidates_query = cur.execute(
        """
        SELECT row_to_json(col)
        FROM (SELECT * FROM candidates) col;
        """
    )

    candidates = [candidate[0] for candidate in cur.fetchall()]
    print(candidates)

    if len(candidates) == 0:
        raise Exception("No candidates found in the database")
    else:
        print(candidates)

    consumer.subscribe(['voters_topic'])

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            elif msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break
            else:
                voter = json.loads(msg.value().decode('utf-8'))
                chosen_candidate = random.choice(candidates)
                vote = voter | chosen_candidate | {
                    'voting_time': datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
                    'vote': 1
                }
                
                try:
                    print(f'user {vote['voter_id']} is voting for candidate: {vote['candidate_id']}')
                    cur.execute(
                        """
                        INSERT INTO votes (voter_id, candidate_id, voting_time)
                        VALUES(%s, %s, %s)
                        """,
                        (
                            vote['voter_id'], vote['candidate_id'], vote['voting_time']
                        )
                    )

                    conn.commit()

                except Exception as e:
                    print(e)
    
    except Exception as e:
        print(e)
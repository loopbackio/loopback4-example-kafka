import {
  KafkaClient,
  HighLevelProducer,
  ProduceRequest,
  ConsumerGroup,
} from 'kafka-node';

import {post, param, requestBody} from '@loopback/rest';

//tslint:disable:no-any

const KAFKA_HOST = '127.0.0.1:9092';

export class KafkaDemoController {
  private client: KafkaClient;
  private producer: HighLevelProducer;

  constructor() {
    this.client = new KafkaClient({kafkaHost: KAFKA_HOST});
    this.producer = new HighLevelProducer(this.client, {});
    this.producer.on('ready', () => {});
  }

  /**
   * Wait for the producer to be ready
   */
  private isProducerReady() {
    return new Promise<void>((resolve, reject) => {
      this.producer.on('ready', () => resolve());
      this.producer.on('error', err => reject(err));
    });
  }

  /**
   * 
   * @param topic 
   */
  @post('/topics/{topic}/subscriptions')
  subscribe(@param.path.string('topic') topic: string) {
    const consumer = new ConsumerGroup(
      {kafkaHost: KAFKA_HOST, groupId: 'KafkaDemoController'},
      [topic],
    );
    consumer.on('message', message => {
      console.log('Message received:', message);
    });
  }

  /**
   * Create topics
   * @param topics
   */
  @post('/topics')
  async createTopics(
    @requestBody({
      content: {
        'application/json': {
          schema: {
            type: 'array',
            items: {type: 'string'},
          },
        },
      },
    })
    topics: string[],
  ) {
    await this.isProducerReady();
    return new Promise<any>((resolve, reject) => {
      this.producer.createTopics(topics, true, (err, data) => {
        if (err) reject(err);
        else resolve(data);
      });
    });
  }

  /**
   * Publish a message to the given topic
   * @param topic The topic name
   * @param message The message
   */
  @post('/topics/{topic}/messages')
  async publish(
    @param.path.string('topic') topic: string,
    @requestBody({
      content: {
        'application/json': {
          schema: {
            type: 'array',
            items: {type: 'string'},
          },
        },
      },
    })
    messages: string[],
  ) {
    await this.isProducerReady();
    const req: ProduceRequest = {topic, messages};
    return new Promise<any>((resolve, reject) => {
      this.producer.send([req], (err, data) => {
        if (err) reject(err);
        else resolve(data);
      });
    });
  }
}

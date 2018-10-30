import {
  KafkaClient,
  HighLevelProducer,
  ProduceRequest,
  ConsumerGroup,
} from 'kafka-node';

import {
  post,
  param,
  requestBody,
  get,
  RestBindings,
  Response,
} from '@loopback/rest';
import {inject} from '@loopback/context';

//tslint:disable:no-any

const KAFKA_HOST = '127.0.0.1:9092';

export class KafkaDemoController {
  private client: KafkaClient;
  private producer: HighLevelProducer;

  constructor(
    @inject('kafka.host', {optional: true})
    private kafkaHost: string = KAFKA_HOST,
  ) {
    this.client = new KafkaClient({kafkaHost});
    this.producer = new HighLevelProducer(this.client, {});
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
  @get('/topics/{topic}/messages', {
    responses: {
      '200': {
        'text/event-stream': {
          schema: {
            type: 'string',
          },
        },
      },
    },
  })
  subscribe(
    @param.path.string('topic') topic: string,
    @param.query.string('limit') limit: number,
    @inject(RestBindings.Http.RESPONSE) response: Response,
  ) {
    limit = +limit || 5;
    response.setHeader('Cache-Control', 'no-cache');
    response.contentType('text/event-stream');
    const consumer = new ConsumerGroup(
      {
        kafkaHost: this.kafkaHost,
        groupId: 'KafkaDemoController',
      },
      [topic],
    );
    let count = 0;
    consumer.on('message', message => {
      count++;
      response.write(`id: ${message.offset}\n`);
      response.write('event: message\n');
      response.write(`data: ${JSON.stringify(message)}\n`);
      if (count >= limit) {
        response.end();
        consumer.close(false, () => {});
      }
    });
    return response;
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

// import { APIGatewayProxyHandler } from 'aws-lambda';
// import 'source-map-support/register';

// export const hello: APIGatewayProxyHandler = async (event, _context) => {
//   return {
//     statusCode: 200,
//     body: JSON.stringify({
//       message: 'Go Serverless Webpack (Typescript) v1.0! Your function executed successfully!',
//       input: event,
//     }, null, 2),
//   };
// }


// Maybe use DynamoDB as Event store?
// https://medium.com/@domagojk/serverless-event-sourcing-in-aws-lambda-dynamodb-sqs-7237d79aed27


// -------------
// GraphQL setup
import { GraphQLServerLambda } from 'graphql-yoga'
// import { depthLimit } from 'graphql-depth-limit'


const typeDefs = './schema.graphql'

const resolvers = {
  Query: {
    hello: (_, { name }) => {
      const returnValue = `Hello ${name || 'World!'}`
      return returnValue
    }
  }
}

//TODO: Upload handler: https://www.npmjs.com/package/graphql-upload

//TODO: Schema directives visitor needed? 
// - https://github.com/Urigo/graphql-modules/issues/381
// - probably not: https://github.com/prisma/graphql-yoga/blob/master/examples/schema-directives/index.js


const lambda = new GraphQLServerLambda({
  typeDefs,
  resolvers,
})

export const server = lambda.graphqlHandler
export const playground = lambda.playgroundHandler


// const serverOptions = {
//   port: 4000,
//   validationRules: [depthLimit(2)]
// }

// server.start(serverOptions, () => console.log('Server is running on http://localhost:4000'))

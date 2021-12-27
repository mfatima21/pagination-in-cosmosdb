import {
  CosmosClient,
  Database,
  FeedOptions,
  ItemResponse,
  SqlQuerySpec,
  RetryOptions,
  OperationInput,
  BulkOptions, RequestOptions,
} from "@azure/cosmos"
import { FilterCondition, SortOrder } from "digicust_types"
import _ from "lodash"

/**
 * Azure CosmosDB Helper
 */
export class CosmosDbHelper {
  private client: CosmosClient

  private db: Database

  /**
   * New CosmosDB Helper Instance
   *
   * @param configuration CosmosDB Configuration, typically ```configuration.cosmosDb```
   */
  constructor(configuration: {
    cosmosEndpoint: string
    cosmosKey: string
    cosmosDbName: string
    retryOptions?: RetryOptions
  }) {
    const { cosmosEndpoint, cosmosKey, retryOptions } = configuration

    const cosmosConfig = {
      endpoint: cosmosEndpoint,
      key: cosmosKey,
    }

    if (retryOptions) {
      cosmosConfig.connectionPolicy = {
        retryOptions: configuration.retryOptions,
      }
    }

    this.client = new CosmosClient(cosmosConfig)
    this.db = this.client.database(configuration.cosmosDbName)
  }


  /**
   * Queries a container for items using SQL
   *
   * @param querySpec SQL query
   * @param containerId specifies the container which to query (e.g. "Case")
   * @param options for the query
   */
  public async queryContainer(
    querySpec: SqlQuerySpec,
    containerId: string,
    options?: FeedOptions
  ) {
    const container = this.db.container(containerId)
    const { resources: result } = await container.items
      .query(querySpec, options)
      .fetchAll()
    return result
  }

  /**
   * Queries a container for items using SQL
   *
   * @param querySpec SQL query
   * @param queryOptions SQL Query Options
   * @param containerId specifies the container which to query (e.g. "Case")
   * @param options for the query
   */
  public async queryContainerNext(
    querySpec: SqlQuerySpec,
    queryOptions: FeedOptions,
    containerId: string
  ) {
    const container = this.db.container(containerId)
    const {
      resources: result,
      hasMoreResults,
      continuationToken,
    } = await container.items.query(querySpec, queryOptions).fetchNext()
    return { result, hasMoreResults, continuationToken }
  }

  /**
   * Get a query iterator that returns a new chunk of data each time it is called.
   *
   * @param querySpec SQL query
   * @param containerId specifies the container which to query (e.g. "Case")
   * @param options for the query
   */
  public getQueryIterator(
    querySpec: SqlQuerySpec,
    containerId: string,
    options?: FeedOptions
  ) {
    const container = this.db.container(containerId)
    return container.items.query(querySpec, options)
  }

  public getFullSearchSql = (searches: Array<string>): Array<string> => {
    const response = []

    searches.forEach((search) => {
      if (search) {
        response.push(`AND LOWER(ToString(backend)) LIKE "%${search.toLowerCase()}%"`)
      }
    })
    return response
  }

  public stringToObject = (path, value, obj) => {
    const parts = path.split(".")
    let part
    const last = parts.pop()
    while ((part = parts.shift())) {
      if (typeof obj[part] != "object") obj[part] = {}
      obj = obj[part]
    }

    obj[last] = value
  }

  public getFilterSql = (
    filters: Array<{
      field: string
      condition: FilterCondition
      value: string | number | boolean
    }>
  ): Array<string> => {
    const response = []

    filters.forEach((filter, index) => {
      const replaced = filter.field.replace(".", " ")
      const first = replaced.split(" ")[0]
      const second = replaced.split(" ")[1]
      if (first === "documents" || first === "submissions") {
        response.push(
          `JOIN (SELECT VALUE t${index} FROM t${index} IN backend.${first} WHERE t${index}.${second} = '${filter.value}')`
        )
      } else if (filter.value === undefined) {
        response.push(
          `AND NOT IS_DEFINED(backend${this.objectToString(filter.field)})`
        )
      } else {
        response.push(
          `AND backend${this.objectToString(
            filter.field
          )} ${this.filterCondition(filter.condition)} ${typeof filter.value === "number" ||
            typeof filter.value === "boolean"
            ? `${filter.value}`
            : `'${filter.value}'`
          }`
        )
      }
    })
    return response
  }

  public objectToString(stringArr: String) {
    let result = ""
    stringArr.split(".").forEach((item) => {
      const str = item === "value" ? "[\"value\"]" : `.${item}`
      result += str
    })
    return result
  }

  public filterCondition(filterCondition: FilterCondition) {
    let result
    switch (filterCondition) {
      case FilterCondition.equals:
        result = "="
        break
      case FilterCondition.greaterThan:
        result = ">"
        break
      case FilterCondition.smallerThan:
        result = "<"
        break
      default:
        break
    }
    return result
  }

  public getSort(sortArr: Array<{ field: string; order: SortOrder }>) {
    return sortArr.map((item, index) => {
      switch (item.order) {
        case SortOrder.ASC:
          if (index === 0) {
            return `ORDER BY backend.${item.field} ASC`
          }
          return `backend.${item.field} ASC`

        case SortOrder.DESC:
          if (index === 0) {
            return `ORDER BY backend.${item.field} DESC`
          }
          return `backend.${item.field} DESC`

        default:
          return ""
      }
    })
  }

  public getSearchQuery = (search: Array<{ field: string, value: string }>): string => {
    if (!search || search.length == 0) return ""

    let result: string = ""
    for (let i = 0; i < search.length; i++) {
      result += `AND LOWER(backend.${search[i].field}) LIKE '%${search[i].value.toLowerCase()}%' `
    }

    return result
  }

  /**
   * Get only these properties in result
   * @param properties Array with properties
   * @returns string
   */
  public getPropertiesQuery(properties: Array<string>): string {
    if (!properties || properties.length == 0) return "*"

    let result: string = ""
    for (let i = 0; i < properties.length; i++) {
      result += `backend.${properties[i]}, `
    }

    return result.substr(0, result.length - 2)
  }

  /**
   * @deprecated
   * Filter result based on properties
   * @param dataArr  - data to extract properties
   * @param properties  - array of properties
   * @returns Array<object>
   */
  public getProperties(dataArr: Array<object>, properties: Array<string>): Promise<Array<object>> {
    return new Promise((resolve, reject) => {
      let data = dataArr

      if (properties.length !== 0) {
        data = dataArr?.map((item) => {
          const fCase: any = {}

          for (let i = 0; i < properties.length; i += 1) {
            const returnedProperty = properties[i]
            const propSplitted = returnedProperty.split(".")

            if (Array.isArray(item[propSplitted[0]])) {
              const first = propSplitted[0]
              const second = propSplitted[1]
              fCase[first] = item[first].map((c) => {
                if (fCase[first]) {
                  return { ...fCase[first][0], [second]: c[second] }
                }
                return { [second]: c[second] }
              })
            } else {
              const value = _.get(item, returnedProperty) || null
              this.stringToObject(returnedProperty, value, fCase)
            }
          }
          return fCase
        })
      }

      resolve(data)
    })
  }

  /**
   * Function to get all string connection for building complex sql Query
   * @param sort Array<{ field: string; order: SortOrder }>
   * @param filter Array<{ field: string; condition: FilterCondition; value: string | number; }>
   * @param search Array<string> (To search in the whole object)
   * @param searchFields Array<{ field: string, value: string }> (To search in a field in the object)
   * @param properties Array<string>
   * @returns { sorts, allProperties, filters, searchObjectQuery, searchFieldQuery }
   */
  public getAllOptionsQuery(
    sort: Array<{ field: string; order: SortOrder }>,
    filter: Array<{ field: string; condition: FilterCondition; value: string | number; }>,
    search: Array<string>,
    searchFields: Array<{ field: string, value: string }>,
    properties: Array<string>) {
    const allProperties = this.getPropertiesQuery(properties)
    const sorts = this.getSort(sort).join(", ")
    const filters = this.getFilterSql(filter).filter((item) => item.includes("AND")).join(" ")
    const searchFieldQuery = this.getSearchQuery(searchFields)
    const searchObjectQuery = this.getFullSearchSql(search).join(" ")

    return { sorts, allProperties, filters, searchObjectQuery, searchFieldQuery }
  }
}

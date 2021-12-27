
export default class CasesService {
  private cosmosDbHelper: CosmosDbHelper;

  constructor(
    cosmosDbHelper: CosmosDbHelper,
  ) {
    this.cosmosDbHelper = cosmosDbHelper;
  }

  // getting permission
  private async getModulePermission(customerId, projectId) {
    const projectPermission = await this.cosmosDbHelper.queryContainer(
      {
        query: `SELECT backend.modules from backend WHERE backend.customerId = @customerId AND backend.projectId = @projectId`,
        parameters: [
          {
            name: "@customerId",
            value: customerId,
          },
          {
            name: "@projectId",
            value: projectId,
          },
        ],
      },
      "Project"
    );

    const modules = projectPermission?.[0]?.modules
      ?.filter((it) => it.active === true)
      .map((it) => it.type);

    return modules;
  }

  /**
   * Get list of all with pagination and sorting
   */

  public async getCaseList(
    customerId: string,
    projectId: string,
    contToken: string = "",
    pageLimit: number = 10,
    sort: Array<{ field: string; order: SortOrder }> = [],
    filter: Array<{
      field: string;
      condition: FilterCondition;
      value: string | number;
    }> = [],
    search: Array<string> = [],
    searchFields: Array<{ field: string; value: string }> = [],
    properties: Array<string> = []
  ): Promise<{
    result: Array<CaseModel>;
    options: Array<string>;
    hasMoreResults: boolean;
    continuationToken: string;
  }> {
    const resFilter = await this.getModulePermission(customerId, projectId);

    const {
      allProperties,
      sorts,
      filters,
      searchObjectQuery,
      searchFieldQuery,
    } = this.cosmosDbHelper.getAllOptionsQuery(
      sort,
      filter,
      search,
      searchFields,
      properties
    );

    const { result, hasMoreResults, continuationToken } =
      await this.cosmosDbHelper.queryContainerNext(
        {
          query: `SELECT * FROM general WHERE general.customerId = @customerId AND general.projectId = @projectId ORDER BY general.createdAt`,
          parameters: [
            {
              name: "@customerId",
              value: customerId,
            },
            {
              name: "@projectId",
              value: projectId,
            },
          ],
        },
        {
          maxItemCount: pageLimit,
          continuationToken: contToken,
          partitionKey: "createdAt",
        },
        "testing"
      );

      console.log(result)
    return {
      result: result.filter((it) => resFilter.indexOf(it.module) !== -1) || [],
      options: [
        "Invoice",
        "Waybill",
        "Delivery Note",
        "Order Confirmation",
        "Packing List",
      ],
      hasMoreResults: hasMoreResults || false,
      continuationToken: continuationToken || "",
    };
  }
}

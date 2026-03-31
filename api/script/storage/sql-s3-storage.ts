import * as q from "q";
import * as stream from "stream";
import {
  DeleteObjectCommand,
  DeleteObjectsCommand,
  ListBucketsCommand,
  ListObjectsV2Command,
  PutObjectCommand,
  S3Client,
} from "@aws-sdk/client-s3";
import { Pool } from "pg";
import * as shortid from "shortid";
import * as storage from "./storage";

export class SqlS3Storage implements storage.Storage {
  private readonly bucketName: string;
  private readonly endpoint: string;
  private readonly s3Client: S3Client;
  private readonly pool: Pool;

  constructor() {
    this.bucketName = process.env.S3_BUCKET_NAME;
    this.endpoint = process.env.S3_ENDPOINT;
    this.s3Client = new S3Client({
      endpoint: process.env.S3_ENDPOINT,
      region: process.env.S3_REGION,
      credentials: {
        accessKeyId: process.env.S3_ACCESS_KEY!,
        secretAccessKey: process.env.S3_SECRET_KEY!,
      },
      forcePathStyle: process.env.S3_FORCE_PATH_STYLE === "true",
    });
    this.pool = new Pool({
      max: 100, // max clients in the pool
      idleTimeoutMillis: 30000, // 30 seconds
      connectionTimeoutMillis: 2000, // 2 seconds
      connectionString: process.env.DATABASE_URL,
    });

    this.init()
      .then(() => {
        console.log("Storage initialized successfully");
      })
      .catch((err) => {
        console.log("Error initializing storage:", err);
      });
  }

  init(): q.Promise<void> {
    const queries = [
      `
    CREATE TABLE IF NOT EXISTS accounts (
      id VARCHAR PRIMARY KEY,
      email TEXT NOT NULL,
      name TEXT,
      azure_ad_id TEXT,
      github_id TEXT,
      microsoft_id TEXT,
      created_time BIGINT NOT NULL
    );
    `,
      `CREATE UNIQUE INDEX IF NOT EXISTS idx_accounts_email
     ON accounts (email);`,

      `
    CREATE TABLE IF NOT EXISTS apps (
      id VARCHAR PRIMARY KEY,
      name TEXT NOT NULL,
      account_id VARCHAR NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
      created_time BIGINT NOT NULL
    );
    `,
      `CREATE INDEX IF NOT EXISTS idx_apps_account_id ON apps(account_id);`,

      `
    CREATE TABLE IF NOT EXISTS collaborators (
      app_id VARCHAR NOT NULL REFERENCES apps(id) ON DELETE CASCADE,
      account_id VARCHAR NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
      email TEXT NOT NULL,
      permission TEXT NOT NULL,
      PRIMARY KEY (app_id, email)
    );
    `,
      `CREATE INDEX IF NOT EXISTS idx_collaborators_account_id ON collaborators(account_id);`,
      `CREATE UNIQUE INDEX IF NOT EXISTS one_owner_per_app
     ON collaborators(app_id)
     WHERE permission = 'Owner';`,

      `
    CREATE TABLE IF NOT EXISTS deployments (
      id VARCHAR PRIMARY KEY,
      app_id VARCHAR NOT NULL REFERENCES apps(id) ON DELETE CASCADE,
      name TEXT NOT NULL,
      key TEXT NOT NULL UNIQUE,
      created_time BIGINT NOT NULL
    );
    `,
      `CREATE INDEX IF NOT EXISTS idx_deployments_app_id ON deployments(app_id);`,

      `
    CREATE TABLE IF NOT EXISTS packages (
      id VARCHAR PRIMARY KEY,
      deployment_id VARCHAR NOT NULL REFERENCES deployments(id) ON DELETE CASCADE,
      app_version TEXT NOT NULL,
      blob_url TEXT NOT NULL,
      description TEXT,
      is_disabled BOOLEAN NOT NULL DEFAULT FALSE,
      is_mandatory BOOLEAN NOT NULL DEFAULT FALSE,
      label TEXT NOT NULL,
      manifest_blob_url TEXT,
      original_deployment VARCHAR,
      original_label TEXT,
      package_hash TEXT NOT NULL,
      released_by TEXT,
      release_method TEXT,
      rollout INTEGER,
      size BIGINT NOT NULL,
      upload_time BIGINT NOT NULL
    );
    `,
      `CREATE INDEX IF NOT EXISTS idx_packages_deployment_id ON packages(deployment_id);`,
      `CREATE INDEX IF NOT EXISTS idx_packages_label ON packages(deployment_id, label);`,
      `CREATE INDEX IF NOT EXISTS idx_packages_hash ON packages(package_hash);`,

      `
    CREATE TABLE IF NOT EXISTS package_diff_map (
      id VARCHAR PRIMARY KEY,
      package_id VARCHAR NOT NULL REFERENCES packages(id) ON DELETE CASCADE,
      package_hash TEXT NOT NULL,
      blob_url TEXT NOT NULL,
      size BIGINT NOT NULL
    );
    `,
      `CREATE INDEX IF NOT EXISTS idx_diff_package_id ON package_diff_map(package_id);`,

      `
    CREATE TABLE IF NOT EXISTS access_keys (
      id VARCHAR PRIMARY KEY,
      account_id VARCHAR NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
      name TEXT NOT NULL UNIQUE,
      friendly_name TEXT  NULL,
      created_by TEXT NOT NULL,
      created_time BIGINT NOT NULL,
      expires BIGINT NOT NULL,
      description TEXT,
      is_session BOOLEAN DEFAULT FALSE
    );
    `,
      `CREATE INDEX IF NOT EXISTS idx_access_keys_account_id ON access_keys(account_id);`,
    ];

    return queries.reduce((prev, query) => {
      return prev.then(() => q(this.pool.query(query)).then(() => {}));
    }, q());
  }

  checkHealth(): q.Promise<void> {
    const checkDb = q(this.pool.query("SELECT 1"))
      .then(() => {})
      .catch((err) => {
        throw new Error("PostgreSQL health check failed: " + err.message);
      });

    const checkS3 = q(this.s3Client.send(new ListBucketsCommand({})))
      .then(() => {})
      .catch((err) => {
        throw new Error("S3 health check failed: " + err.message);
      });

    return q.all([checkDb, checkS3]).then(() => {});
  }
  addAccount(account: storage.Account): q.Promise<string> {
    account = storage.clone(account);
    account.id = shortid();
    account.createdTime = new Date().getTime();

    return q(this.pool.query("SELECT id FROM accounts WHERE email = $1", [account.email.toLowerCase()]))
      .then((existing: any) => {
        if (existing.rows.length > 0) {
          return q.reject(storage.storageError(storage.ErrorCode.AlreadyExists));
        }

        return q(
          this.pool.query(
            `
        INSERT INTO accounts (
          id,
          email,
          name,
          azure_ad_id,
          github_id,
          microsoft_id,
          created_time
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        `,
            [
              account.id,
              account.email.toLowerCase(),
              account.name,
              account.azureAdId,
              account.gitHubId,
              account.microsoftId,
              account.createdTime,
            ]
          )
        );
      })
      .then(() => {
        return account.id;
      })
      .catch((err: any) => {
        if (err.code === "23505") {
          return q.reject(storage.storageError(storage.ErrorCode.AlreadyExists));
        }
        return q.reject(err);
      });
  }
  getAccount(accountId: string): q.Promise<storage.Account> {
    return q(this.pool.query("SELECT * FROM accounts WHERE id = $1", [accountId]))
      .then((result: any) => {
        if (result.rows.length === 0) {
          return q.reject(storage.storageError(storage.ErrorCode.NotFound));
        }
        const row = result.rows[0];
        return this.mapAccount(row);
      })
      .catch((err: any) => {
        return q.reject(err);
      });
  }
  getAccountByEmail(email: string): q.Promise<storage.Account> {
    return q(this.pool.query("SELECT * FROM accounts WHERE email = $1", [email.toLowerCase()]))
      .then((result: any) => {
        if (result.rows.length === 0) {
          return q.reject(storage.storageError(storage.ErrorCode.NotFound));
        }
        const row = result.rows[0];
        return this.mapAccount(row);
      })
      .catch((err: any) => {
        return q.reject(err);
      });
  }
  getAccountIdFromAccessKey(accessKey: string): q.Promise<string> {
    return q(this.pool.query("SELECT account_id, expires FROM access_keys WHERE name = $1", [accessKey]))
      .then((result: any) => {
        if (result.rows.length === 0) {
          return q.reject(storage.storageError(storage.ErrorCode.NotFound));
        }

        const row = result.rows[0];

        if (new Date().getTime() >= Number(row.expires)) {
          return q.reject(storage.storageError(storage.ErrorCode.Expired, "The access key has expired."));
        }

        return row.account_id;
      })
      .catch((err: any) => {
        return q.reject(err);
      });
  }
  updateAccount(email: string, updates: storage.Account): q.Promise<void> {
    if (!email) {
      throw new Error("No account email");
    }

    return this.getAccountByEmail(email.toLowerCase())
      .then((account: storage.Account) => {
        const fields: string[] = [];
        const values: any[] = [];
        let index = 1;

        if (updates.name !== undefined) {
          fields.push(`name = $${index++}`);
          values.push(updates.name);
        }

        if (updates.email !== undefined) {
          fields.push(`email = $${index++}`);
          values.push(updates.email.toLowerCase());
        }

        if (updates.azureAdId !== undefined) {
          fields.push(`azure_ad_id = $${index++}`);
          values.push(updates.azureAdId);
        }

        if (updates.gitHubId !== undefined) {
          fields.push(`github_id = $${index++}`);
          values.push(updates.gitHubId);
        }

        if (updates.microsoftId !== undefined) {
          fields.push(`microsoft_id = $${index++}`);
          values.push(updates.microsoftId);
        }

        if (fields.length === 0) {
          return;
        }

        values.push(account.id);

        return q(
          this.pool.query(
            `
        UPDATE accounts
        SET ${fields.join(", ")}
        WHERE id = $${index}
        `,
            values
          )
        );
      })
      .then(() => undefined)
      .catch((err: any) => q.reject(err));
  }
  addApp(accountId: string, app: storage.App): q.Promise<storage.App> {
    app = storage.clone(app);
    app.id = shortid();
    app.createdTime = new Date().getTime();

    return this.getAccount(accountId)
      .then((account: storage.Account) => {
        return q(this.pool.query("BEGIN"))
          .then(() => {
            return q(
              this.pool.query(
                `
            INSERT INTO apps (id, name, account_id, created_time)
            VALUES ($1, $2, $3, $4)
            `,
                [app.id, app.name, accountId, app.createdTime]
              )
            );
          })
          .then(() => {
            return q(
              this.pool.query(
                `
            INSERT INTO collaborators (app_id, account_id, email, permission)
            VALUES ($1, $2, $3, $4)
            `,
                [app.id, accountId, account.email.toLowerCase(), storage.Permissions.Owner]
              )
            );
          })
          .then(() => {
            return q(this.pool.query("COMMIT"));
          })
          .then(() => {
            app.collaborators = {
              [account.email.toLowerCase()]: {
                accountId: accountId,
                permission: storage.Permissions.Owner,
              },
            };

            return storage.clone(app);
          })
          .catch((err: any) => {
            return q(this.pool.query("ROLLBACK")).then(() => q.reject(err));
          });
      })
      .catch((err: any) => q.reject(err));
  }
  getApps(accountId: string): q.Promise<storage.App[]> {
    return q(this.getAccount(accountId))
      .then((account: storage.Account) => {
        if (!account) {
          return q.reject(storage.storageError(storage.ErrorCode.NotFound, "Account does not exist"));
        }

        return q(
          this.pool.query(
            `
              SELECT app_id, account_id, email, permission
              FROM collaborators
              WHERE account_id = $1
            `,
            [account.id]
          )
        );
      })
      .then((result: any) => {
        if (!result || result.rows.length === 0) {
          return [];
        }

        const appIds = result.rows.map((r: any) => r.app_id);

        return q
          .all([
            this.pool.query(
              `
          SELECT * FROM apps
          WHERE id = ANY($1)
          `,
              [appIds]
            ),
            this.pool.query(
              `
              SELECT app_id, account_id, email, permission
              FROM collaborators
              WHERE app_id = ANY($1)
            `,
              [appIds]
            ),
          ])
          .then(([app, collaborator]: any) => {
            if (!app || app.rows.length === 0) {
              return [];
            }
            const mapCollaborator: { [appId: string]: storage.CollaboratorMap } = {};
            collaborator.rows.forEach((row: any) => {
              if (!mapCollaborator[row.app_id]) {
                mapCollaborator[row.app_id] = {};
              }
              mapCollaborator[row.app_id][row.email.toLowerCase()] = this.mapCollaboratorProperties(row);
            });

            const apps: storage.App[] = app.rows.map((row: any) => {
              const app = this.mapApp(row);
              app.collaborators = mapCollaborator[app.id] || {};

              this.addIsCurrentAccountProperty(app, accountId);

              return app;
            });

            return apps;
          });
      })
      .catch((err: any) => q.reject(err));
  }
  getApp(accountId: string, appId: string): q.Promise<storage.App> {
    return q
      .all([this.getAccount(accountId), q(this.pool.query("SELECT * FROM apps WHERE id = $1", [appId]))])
      .then(([_, result]: any) => {
        if (result.rows.length === 0) {
          return q.reject(storage.storageError(storage.ErrorCode.NotFound));
        }

        const row = result.rows[0];

        const app = this.mapApp(row);

        return q(this.pool.query("SELECT account_id, email, permission FROM collaborators WHERE app_id = $1", [app.id])).then(
          (collaborator: any) => {
            app.collaborators = this.mapCollaboratorMap(collaborator.rows);
            this.addIsCurrentAccountProperty(app, accountId);

            return app;
          }
        );
      })
      .catch((err: any) => q.reject(err));
  }
  removeApp(accountId: string, appId: string): q.Promise<void> {
    return q
      .all([this.getAccount(accountId), q(this.pool.query("SELECT account_id FROM apps WHERE id = $1", [appId]))])
      .then(([_, result]: any) => {
        if (result.rows.length === 0) {
          return q.reject(storage.storageError(storage.ErrorCode.NotFound));
        }

        const ownerId = result.rows[0].account_id;

        if (ownerId !== accountId) {
          throw new Error("Wrong accountId");
        }

        return q(this.pool.query("BEGIN"));
      })
      .then(() => {
        return q(this.pool.query("SELECT id FROM deployments WHERE app_id = $1", [appId]));
      })
      .then((deployment: any) => {
        const promises = deployment.rows.map((d: any) => {
          return this.removeDeployment(accountId, appId, d.id);
        });

        return q.all(promises);
      })
      .then(() => {
        return q(this.pool.query("DELETE FROM collaborators WHERE app_id = $1", [appId]));
      })
      .then(() => {
        return q(this.pool.query("DELETE FROM apps WHERE id = $1", [appId]));
      })
      .then(() => {
        return q(this.pool.query("COMMIT"));
      })
      .then(() => {
        return;
      })
      .catch((err: any) => {
        return q(this.pool.query("ROLLBACK")).then(() => q.reject(err));
      });
  }
  transferApp(accountId: string, appId: string, email: string): q.Promise<void> {
    if (storage.isPrototypePollutionKey(email.toLowerCase())) {
      return q.reject(storage.storageError(storage.ErrorCode.Invalid, "Invalid email parameter"));
    }

    let requesterEmail: string;
    let targetAccountId: string;
    let targetEmail: string;

    return this.getApp(accountId, appId)
      .then((_) => {
        return this.getAccount(accountId)
          .then((account: storage.Account) => {
            requesterEmail = account.email.toLowerCase();

            return q(this.pool.query("SELECT id, email FROM accounts WHERE email = $1", [email.toLowerCase()]));
          })
          .then((account: any) => {
            if (account.rows.length === 0) {
              return q.reject(
                storage.storageError(storage.ErrorCode.NotFound, "The specified e-mail address doesn't represent a registered user")
              );
            }

            targetAccountId = account.rows[0].id;
            targetEmail = account.rows[0].email.toLowerCase();

            return q(
              this.pool.query(
                `
            SELECT permission FROM collaborators
            WHERE app_id = $1 AND email = $2
            `,
                [appId, targetEmail.toLowerCase()]
              )
            );
          })
          .then((collaborator: any) => {
            if (collaborator.rows.length > 0 && collaborator.rows[0].permission === storage.Permissions.Owner) {
              return q.reject(storage.storageError(storage.ErrorCode.AlreadyExists));
            }

            return q(this.pool.query("BEGIN"));
          })
          .then(() => {
            return q(
              this.pool.query(
                `
            UPDATE collaborators
            SET permission = $1
            WHERE app_id = $2 AND email = $3
            `,
                [storage.Permissions.Collaborator, appId, requesterEmail.toLowerCase()]
              )
            );
          })
          .then(() => {
            return q(
              this.pool.query(
                `
            SELECT * FROM collaborators
            WHERE app_id = $1 AND email = $2
            `,
                [appId, targetEmail.toLowerCase()]
              )
            );
          })
          .then((collaborator: any) => {
            if (collaborator.rows.length > 0) {
              return q(
                this.pool.query(
                  `
              UPDATE collaborators
              SET permission = $1
              WHERE app_id = $2 AND email = $3
              `,
                  [storage.Permissions.Owner, appId, targetEmail.toLowerCase()]
                )
              );
            } else {
              return q(
                this.pool.query(
                  `
              INSERT INTO collaborators (app_id, account_id, email, permission)
              VALUES ($1, $2, $3, $4)
              `,
                  [appId, targetAccountId, targetEmail.toLowerCase(), storage.Permissions.Owner]
                )
              );
            }
          })
          .then(() => q(this.pool.query("COMMIT")))
          .then(() => undefined)
          .catch((err: any) => {
            return q(this.pool.query("ROLLBACK")).then(() => q.reject(err));
          });
      })
      .catch((err: any) => q.reject(err));
  }
  updateApp(accountId: string, updates: storage.App, ensureIsOwner: boolean = true): q.Promise<void> {
    updates = storage.clone(updates);

    return q
      .all([this.getAccount(accountId), q(this.pool.query("SELECT id FROM apps WHERE id = $1", [updates.id]))])
      .then(([_, app]: any) => {
        if (app.rows.length === 0) {
          return q.reject(storage.storageError(storage.ErrorCode.NotFound));
        }

        if (ensureIsOwner) {
          return q(
            this.pool.query(
              `
          SELECT 1 FROM collaborators
          WHERE app_id = $1 AND account_id = $2 AND permission = $3
          `,
              [updates.id, accountId, storage.Permissions.Owner]
            )
          ).then((collaborator: any) => {
            if (collaborator.rows.length === 0) {
              throw new Error("Not owner");
            }
          });
        }
      })
      .then(() => {
        this.removeIsCurrentAccountProperty(updates);

        return q(this.pool.query("BEGIN"));
      })
      .then(() => {
        const queries: any[] = [];

        const fields: string[] = [];
        const values: any[] = [];
        let i = 1;

        if (updates.name !== undefined) {
          fields.push(`name = $${i++}`);
          values.push(updates.name);
        }

        if (fields.length > 0) {
          values.push(updates.id);

          queries.push(q(this.pool.query(`UPDATE apps SET ${fields.join(", ")} WHERE id = $${i}`, values)));
        }

        if (updates.collaborators) {
          Object.keys(updates.collaborators).forEach((email) => {
            const c = updates.collaborators[email.toLowerCase()];

            queries.push(
              q(
                this.pool.query(
                  `
              INSERT INTO collaborators (app_id, account_id, email, permission)
              VALUES ($1, $2, $3, $4)
              ON CONFLICT (app_id, email)
              DO UPDATE SET
                account_id = EXCLUDED.account_id,
                permission = EXCLUDED.permission
              `,
                  [updates.id, c.accountId, email.toLowerCase(), c.permission]
                )
              )
            );
          });
        }

        return q.all(queries);
      })
      .then(() => {
        return q(this.pool.query("COMMIT"));
      })
      .then(() => undefined)
      .catch((err: any) => {
        return q(this.pool.query("ROLLBACK")).then(() => q.reject(err));
      });
  }
  addCollaborator(accountId: string, appId: string, email: string): q.Promise<void> {
    if (storage.isPrototypePollutionKey(email.toLowerCase())) {
      return q.reject(storage.storageError(storage.ErrorCode.Invalid, "Invalid email parameter"));
    }

    let targetAccountId: string;
    let targetEmail: string;

    return this.getApp(accountId, appId)
      .then(() => {
        return q(this.pool.query("SELECT id, email FROM accounts WHERE email = $1", [email.toLowerCase()]));
      })
      .then((account: any) => {
        if (account.rows.length === 0) {
          return q.reject(
            storage.storageError(storage.ErrorCode.NotFound, "The specified e-mail address doesn't represent a registered user")
          );
        }

        targetAccountId = account.rows[0].id;
        targetEmail = account.rows[0].email.toLowerCase();

        return q(
          this.pool.query(
            `
        SELECT permission FROM collaborators
        WHERE app_id = $1 AND email = $2
        `,
            [appId, targetEmail.toLowerCase()]
          )
        );
      })
      .then((collabResult: any) => {
        if (collabResult.rows.length > 0) {
          return q.reject(storage.storageError(storage.ErrorCode.AlreadyExists));
        }

        return q(this.pool.query("BEGIN"));
      })
      .then(() => {
        return q(
          this.pool.query(
            `
        INSERT INTO collaborators (app_id, account_id, email, permission)
        VALUES ($1, $2, $3, $4)
        `,
            [appId, targetAccountId, targetEmail, "Collaborator"]
          )
        );
      })
      .then(() => q(this.pool.query("COMMIT")))
      .then(() => undefined)
      .catch((err: any) => {
        return q(this.pool.query("ROLLBACK")).then(() => q.reject(err));
      });
  }
  getCollaborators(accountId: string, appId: string): q.Promise<storage.CollaboratorMap> {
    return this.getApp(accountId, appId)
      .then(() => {
        return q(
          this.pool.query(
            `
        SELECT account_id, email, permission
        FROM collaborators
        WHERE app_id = $1
        `,
            [appId]
          )
        );
      })
      .then((result: any) => {
        return this.mapCollaboratorMap(result.rows);
      })
      .catch((err: any) => q.reject(err));
  }
  removeCollaborator(accountId: string, appId: string, email: string): q.Promise<void> {
    let targetEmail: string;

    return this.getApp(accountId, appId)
      .then((app: storage.App) => {
        if (this.isOwner(app.collaborators, email.toLowerCase())) {
          return q.reject(storage.storageError(storage.ErrorCode.AlreadyExists));
        }

        return q(this.pool.query("SELECT id, email FROM accounts WHERE email = $1", [email.toLowerCase()]))
          .then((account: any) => {
            if (account.rows.length === 0) {
              return q.reject(storage.storageError(storage.ErrorCode.NotFound));
            }

            targetEmail = account.rows[0].email.toLowerCase();

            return q(
              this.pool.query(
                `
            SELECT 1 FROM collaborators
            WHERE app_id = $1 AND email = $2
            `,
                [appId, targetEmail.toLowerCase()]
              )
            );
          })
          .then((collaborator: any) => {
            if (collaborator.rows.length === 0) {
              return q.reject(storage.storageError(storage.ErrorCode.NotFound));
            }

            return q(this.pool.query("BEGIN"));
          })
          .then(() => {
            return q(
              this.pool.query(
                `
            DELETE FROM collaborators
            WHERE app_id = $1 AND email = $2
            `,
                [appId, targetEmail.toLowerCase()]
              )
            );
          })
          .then(() => q(this.pool.query("COMMIT")))
          .then(() => undefined)
          .catch((err: any) => {
            return q(this.pool.query("ROLLBACK")).then(() => q.reject(err));
          });
      })
      .catch((err: any) => q.reject(err));
  }
  addDeployment(accountId: string, appId: string, deployment: storage.Deployment): q.Promise<string> {
    deployment = storage.clone(deployment);
    deployment.id = shortid();
    deployment.createdTime = new Date().getTime();

    return q
      .all([this.getAccount(accountId), q(this.pool.query("SELECT id FROM apps WHERE id = $1", [appId]))])
      .then(([_, app]: any) => {
        if (app.rows.length === 0) {
          return q.reject(storage.storageError(storage.ErrorCode.NotFound));
        }

        return q(this.pool.query("BEGIN"));
      })
      .then(() => {
        return q(
          this.pool.query(
            `
        INSERT INTO deployments (
          id,
          app_id,
          name,
          key,
          created_time
        )
        VALUES ($1, $2, $3, $4, $5)
        `,
            [deployment.id, appId, deployment.name, deployment.key, deployment.createdTime]
          )
        );
      })
      .then(() => q(this.pool.query("COMMIT")))
      .then(() => deployment.id)
      .catch((err: any) => {
        return q(this.pool.query("ROLLBACK")).then(() => {
          if (err.code === "23505") {
            return q.reject(storage.storageError(storage.ErrorCode.AlreadyExists));
          }
          return q.reject(err);
        });
      });
  }
  getDeployment(accountId: string, appId: string, deploymentId: string): q.Promise<storage.Deployment> {
    return q
      .all([
        this.getAccount(accountId),
        q(this.pool.query("SELECT id FROM apps WHERE id = $1", [appId])),
        q(
          this.pool.query(
            `
      SELECT id, name, key, created_time
      FROM deployments
      WHERE id = $1
      `,
            [deploymentId]
          )
        ),
      ])
      .then(([_, app, deployment]: any) => {
        if (app.rows.length === 0 || deployment.rows.length === 0) {
          return q.reject(storage.storageError(storage.ErrorCode.NotFound));
        }

        const row = deployment.rows[0];

        return this.mapDeployment(row);
      })
      .catch((err: any) => q.reject(err));
  }
  getDeploymentInfo(deploymentKey: string): q.Promise<storage.DeploymentInfo> {
    return q(
      this.pool.query(
        `
    SELECT id AS deployment_id, app_id
    FROM deployments
    WHERE key = $1
    `,
        [deploymentKey]
      )
    )
      .then((result: any) => {
        if (result.rows.length === 0) {
          return q.reject(storage.storageError(storage.ErrorCode.NotFound));
        }

        const row = result.rows[0];

        return {
          deploymentId: row.deployment_id,
          appId: row.app_id,
        };
      })
      .catch((err: any) => q.reject(err));
  }
  getDeployments(accountId: string, appId: string): q.Promise<storage.Deployment[]> {
    return q
      .all([
        this.getAccount(accountId),
        q(
          this.pool.query(
            `
      SELECT id, name, key, created_time
      FROM deployments
      WHERE app_id = $1
      `,
            [appId]
          )
        ),
      ])
      .then(([_, result]: any) => {
        if (result.rows.length === 0) {
          return q.reject(storage.storageError(storage.ErrorCode.NotFound));
        }

        return result.rows.map((row: any) => this.mapDeployment(row));
      })
      .catch((err: any) => q.reject(err));
  }
  removeDeployment(accountId: string, appId: string, deploymentId: string): q.Promise<void> {
    return q
      .all([
        this.getAccount(accountId),
        q(this.pool.query("SELECT id FROM apps WHERE id = $1", [appId])),
        q(
          this.pool.query(
            `
      SELECT app_id FROM deployments
      WHERE id = $1
      `,
            [deploymentId]
          )
        ),
      ])
      .then(([_, app, deployment]: any) => {
        if (app.rows.length === 0 || deployment.rows.length === 0) {
          return q.reject(storage.storageError(storage.ErrorCode.NotFound));
        }

        const actualAppId = deployment.rows[0].app_id;

        if (actualAppId !== appId) {
          throw new Error("Wrong appId");
        }

        return q(this.pool.query("BEGIN"));
      })
      .then(() => {
        return q(
          this.pool.query(
            `
        DELETE FROM deployments
        WHERE id = $1
        `,
            [deploymentId]
          )
        );
      })
      .then(() => q(this.pool.query("COMMIT")))
      .then(() => undefined)
      .catch((err: any) => {
        return q(this.pool.query("ROLLBACK")).then(() => q.reject(err));
      });
  }
  updateDeployment(accountId: string, appId: string, deployment: storage.Deployment): q.Promise<void> {
    deployment = storage.clone(deployment);
    delete deployment.package;

    return q
      .all([
        this.getAccount(accountId),
        q(this.pool.query("SELECT id FROM apps WHERE id = $1", [appId])),
        q(this.pool.query("SELECT id FROM deployments WHERE id = $1", [deployment.id])),
      ])
      .then(([_, app, deployment]: any) => {
        if (app.rows.length === 0 || deployment.rows.length === 0) {
          return q.reject(storage.storageError(storage.ErrorCode.NotFound));
        }

        return q(this.pool.query("BEGIN"));
      })
      .then(() => {
        const fields: string[] = [];
        const values: any[] = [];
        let i = 1;

        if (deployment.name !== undefined) {
          fields.push(`name = $${i++}`);
          values.push(deployment.name);
        }

        if (deployment.key !== undefined) {
          fields.push(`key = $${i++}`);
          values.push(deployment.key);
        }

        if (fields.length === 0) {
          return;
        }

        values.push(deployment.id);

        return q(
          this.pool.query(
            `
        UPDATE deployments
        SET ${fields.join(", ")}
        WHERE id = $${i}
        `,
            values
          )
        );
      })
      .then(() => q(this.pool.query("COMMIT")))
      .then(() => undefined)
      .catch((err: any) => {
        return q(this.pool.query("ROLLBACK")).then(() => {
          if (err.code === "23505") {
            return q.reject(storage.storageError(storage.ErrorCode.AlreadyExists));
          }
          return q.reject(err);
        });
      });
  }
  commitPackage(accountId: string, appId: string, deploymentId: string, updates: storage.Package): q.Promise<storage.Package> {
    updates = storage.clone(updates);

    if (!updates) {
      throw new Error("No package specified");
    }

    let newLabel: string;

    return (
      q
        .all([
          this.getAccount(accountId),
          this.pool.query("SELECT id, account_id FROM apps WHERE id = $1", [appId]),
          this.pool.query("SELECT id FROM deployments WHERE id = $1 AND app_id = $2", [deploymentId, appId]),
        ])
        .then(([_, app, deployment]: any) => {
          if (app.rows.length === 0 || deployment.rows.length === 0 || app.rows[0].account_id !== accountId) {
            return q.reject(storage.storageError(storage.ErrorCode.NotFound));
          }

          return this.pool.query("BEGIN");
        })

        // 🔒 LOCK packages của deployment để tránh race condition
        .then(() => {
          return this.pool.query(
            `
        SELECT label
        FROM packages
        WHERE deployment_id = $1
        ORDER BY upload_time DESC
        LIMIT 1
        FOR UPDATE
        `,
            [deploymentId]
          );
        })

        .then((pkg: any) => {
          if (pkg.rows.length > 0) {
            const lastLabel = pkg.rows[0].label; // vd: v5
            const lastVersion = Number(lastLabel.replace("v", ""));
            newLabel = "v" + (lastVersion + 1);
          } else {
            newLabel = "v1";
          }

          updates.label = newLabel;
          updates.uploadTime = Date.now();

          // disable rollout package trước đó
          return this.pool.query(
            `
        UPDATE packages
        SET rollout = NULL
        WHERE deployment_id = $1
        `,
            [deploymentId]
          );
        })

        .then(() => {
          return this.pool.query(
            `
        INSERT INTO packages (
          id,
          deployment_id,
          app_version,
          blob_url,
          description,
          is_disabled,
          is_mandatory,
          label,
          manifest_blob_url,
          original_deployment,
          original_label,
          package_hash,
          released_by,
          release_method,
          rollout,
          size,
          upload_time
        )
        VALUES (
          $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17
        )
        RETURNING *
        `,
            [
              shortid(),
              deploymentId,
              updates.appVersion,
              updates.blobUrl,
              updates.description,
              updates.isDisabled ?? false,
              updates.isMandatory ?? false,
              updates.label,
              updates.manifestBlobUrl ?? null,
              updates.originalDeployment ?? null,
              updates.originalLabel ?? null,
              updates.packageHash,
              updates.releasedBy ?? null,
              updates.releaseMethod ?? storage.ReleaseMethod.Upload,
              updates.rollout ?? null,
              updates.size,
              updates.uploadTime,
            ]
          );
        })

        .then(() => this.pool.query("COMMIT"))

        .then(() => updates)

        .catch((err: any) => {
          return this.pool
            .query("ROLLBACK")
            .catch(() => {}) // tránh crash nếu rollback fail
            .then(() => q.reject(err));
        })
    );
  }
  clearPackageHistory(accountId: string, appId: string, deploymentId: string): q.Promise<void> {
    let deploymentExists = false;

    return q
      .all([
        this.getAccount(accountId),
        q(this.pool.query("SELECT id FROM apps WHERE id = $1", [appId])),
        q(this.pool.query("SELECT id FROM deployments WHERE id = $1", [deploymentId])),
      ])
      .then(([_, app, deployment]: any) => {
        if (app.rows.length === 0 || deployment.rows.length === 0) {
          return q.reject(storage.storageError(storage.ErrorCode.NotFound));
        }

        deploymentExists = true;

        return q(this.pool.query("BEGIN"));
      })
      .then(() => {
        return q(this.pool.query(`DELETE FROM packages WHERE deployment_id = $1`, [deploymentId]));
      })
      .then(() => {
        return q(this.pool.query("COMMIT"));
      })
      .catch((err: any) => {
        if (deploymentExists) {
          return q(this.pool.query("ROLLBACK")).then(() => q.reject(err));
        }
        return q.reject(err);
      })
      .then(() => undefined);
  }
  getPackageHistoryFromDeploymentKey(deploymentKey: string): q.Promise<storage.Package[]> {
    return q(
      this.pool.query(
        `SELECT id
     FROM deployments
     WHERE key = $1`,
        [deploymentKey]
      )
    )
      .then((deployment: any) => {
        if (deployment.rows.length === 0) {
          return q.reject(storage.storageError(storage.ErrorCode.NotFound));
        }

        const deploymentId = deployment.rows[0].id;

        return q(
          this.pool.query(
            `
            SELECT *
            FROM packages
            WHERE deployment_id = $1
            ORDER BY upload_time ASC
          `,
            [deploymentId]
          )
        );
      })
      .then((pkg: any) => {
        return pkg.rows.map((row: any) => {
          return this.mapPackage(row);
        });
      });
  }
  getPackageHistory(accountId: string, appId: string, deploymentId: string): q.Promise<storage.Package[]> {
    return q
      .all([
        this.getAccount(accountId),
        q(this.pool.query("SELECT id FROM apps WHERE id = $1", [appId])),
        q(this.pool.query("SELECT id FROM deployments WHERE id = $1", [deploymentId])),
      ])
      .then(([_, app, deployment]: any) => {
        if (app.rows.length === 0 || deployment.rows.length === 0) {
          return q.reject(storage.storageError(storage.ErrorCode.NotFound));
        }

        return q(
          this.pool.query(
            `
      SELECT *
      FROM packages
      WHERE deployment_id = $1
      ORDER BY upload_time DESC
      `,
            [deploymentId]
          )
        );
      })
      .then((pkg: any) => {
        return pkg.rows.map((row: any) => {
          return this.mapPackage(row);
        });
      });
  }
  public updatePackageHistory(accountId: string, appId: string, deploymentId: string, history: storage.Package[]): q.Promise<void> {
    if (!history || !history.length) {
      return q.reject(storage.storageError(storage.ErrorCode.Invalid, "Cannot clear package history from an update operation"));
    }

    let deploymentExists = false;

    return q
      .all([
        this.getAccount(accountId),
        q(this.pool.query("SELECT id FROM apps WHERE id = $1", [appId])),
        q(this.pool.query("SELECT id FROM deployments WHERE id = $1", [deploymentId])),
      ])
      .then(([_, app, deployment]: any) => {
        if (app.rows.length === 0 || deployment.rows.length === 0) {
          return q.reject(storage.storageError(storage.ErrorCode.NotFound));
        }

        deploymentExists = true;
        return q(this.pool.query("BEGIN"));
      })
      .then(() => {
        const inserts = history.map((pkg) => {
          return q(
            this.pool.query(
              `
        INSERT INTO packages (
          id, deployment_id, app_version, blob_url, description,
          is_disabled, is_mandatory, label, manifest_blob_url,
          original_deployment, original_label, package_hash,
          released_by, release_method, rollout, size, upload_time
        )
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)
        ON CONFLICT (id) DO UPDATE SET
          app_version = EXCLUDED.app_version,
          blob_url = EXCLUDED.blob_url,
          description = EXCLUDED.description,
          is_disabled = EXCLUDED.is_disabled,
          is_mandatory = EXCLUDED.is_mandatory,
          label = EXCLUDED.label,
          manifest_blob_url = EXCLUDED.manifest_blob_url,
          original_deployment = EXCLUDED.original_deployment,
          original_label = EXCLUDED.original_label,
          package_hash = EXCLUDED.package_hash,
          released_by = EXCLUDED.released_by,
          release_method = EXCLUDED.release_method,
          rollout = EXCLUDED.rollout,
          size = EXCLUDED.size,
          upload_time = EXCLUDED.upload_time
        `,
              [
                shortid(),
                deploymentId,
                pkg.appVersion,
                pkg.blobUrl,
                pkg.description,
                pkg.isDisabled || false,
                pkg.isMandatory || false,
                pkg.label,
                pkg.manifestBlobUrl,
                pkg.originalDeployment || null,
                pkg.originalLabel || null,
                pkg.packageHash,
                pkg.releasedBy || null,
                pkg.releaseMethod || storage.ReleaseMethod.Upload,
                pkg.rollout ?? null,
                pkg.size,
                pkg.uploadTime,
              ]
            )
          );
        });

        return q.all(inserts);
      })
      .then(() => q(this.pool.query("COMMIT")))
      .catch((err) => {
        if (deploymentExists) {
          return q(this.pool.query("ROLLBACK")).then(() => q.reject(err));
        }
        return q.reject(err);
      });
  }
  addBlob(blobId: string, blobStream: stream.Readable, streamLength: number): q.Promise<string> {
    if (!this.s3Client) {
      return q.reject(new Error("S3 client not initialized"));
    }

    const command = new PutObjectCommand({
      Bucket: this.bucketName,
      Key: blobId,
      Body: blobStream,
      ContentLength: streamLength,
    });

    return q(this.s3Client.send(command))
      .then(() => {
        return blobId;
      })
      .catch((err) => {
        return q.reject(err);
      });
  }
  getBlobUrl(blobId: string): q.Promise<string> {
    const cleanEndpoint = this.endpoint.replace(/\/+$/, "");

    const blobUrl = `${cleanEndpoint}/${this.bucketName}/${blobId}`;
    return q(blobUrl);
  }
  removeBlob(blobId: string): q.Promise<void> {
    if (!this.s3Client) {
      return q.reject(new Error("S3 client not initialized"));
    }

    const command = new DeleteObjectCommand({
      Bucket: this.bucketName,
      Key: blobId,
    });

    return q(this.s3Client.send(command))
      .then(() => undefined)
      .catch((err) => q.reject(err));
  }
  addAccessKey(accountId: string, accessKey: storage.AccessKey): q.Promise<string> {
    accessKey = storage.clone(accessKey);
    accessKey.id = shortid();
    accessKey.createdTime = new Date().getTime();

    return q(this.pool.query("SELECT id FROM accounts WHERE id = $1", [accountId]))
      .then((accResult: any) => {
        if (accResult.rows.length === 0) {
          return q.reject(storage.storageError(storage.ErrorCode.NotFound));
        }

        return q(
          this.pool.query(
            `
        INSERT INTO access_keys (id, account_id, name, friendly_name, created_by, created_time, expires)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        `,
            [
              accessKey.id,
              accountId,
              accessKey.name,
              accessKey.friendlyName,
              accessKey.createdBy,
              accessKey.createdTime,
              accessKey.expires,
            ]
          )
        );
      })
      .then(() => accessKey.id)
      .catch((err) => q.reject(err));
  }
  getAccessKey(accountId: string, accessKeyId: string): q.Promise<storage.AccessKey> {
    return q(
      this.pool.query(
        `
    SELECT id, account_id, name, friendly_name, created_by, created_time, expires
    FROM access_keys
    WHERE id = $1 AND account_id = $2
    `,
        [accessKeyId, accountId]
      )
    ).then((result: any) => {
      if (result.rows.length === 0) {
        return q.reject(storage.storageError(storage.ErrorCode.NotFound));
      }

      const row = result.rows[0];

      return this.mapAccessKey(row);
    });
  }
  getAccessKeys(accountId: string): q.Promise<storage.AccessKey[]> {
    return q(this.pool.query("SELECT id FROM accounts WHERE id = $1", [accountId]))
      .then((accResult: any) => {
        if (accResult.rows.length === 0) {
          return q.reject(storage.storageError(storage.ErrorCode.NotFound));
        }

        return q(
          this.pool.query(
            `
      SELECT id, account_id, name, friendly_name, created_by, created_time, expires
      FROM access_keys
      WHERE account_id = $1
      ORDER BY created_time ASC
      `,
            [accountId]
          )
        );
      })
      .then((result: any) => {
        if (!result.rows || result.rows.length === 0) {
          return [];
        }

        return result.rows.map((row: any) => {
          return this.mapAccessKey(row);
        });
      });
  }
  removeAccessKey(accountId: string, accessKeyId: string): q.Promise<void> {
    return q(
      this.pool.query(
        `
    SELECT id
    FROM access_keys
    WHERE id = $1 AND account_id = $2
    `,
        [accessKeyId, accountId]
      )
    )
      .then((result: any) => {
        if (result.rows.length === 0) {
          return q.reject(storage.storageError(storage.ErrorCode.NotFound));
        }

        return q(this.pool.query(`DELETE FROM access_keys WHERE id = $1 AND account_id = $2`, [accessKeyId, accountId]));
      })
      .then(() => undefined);
  }
  updateAccessKey(accountId: string, accessKey: storage.AccessKey): q.Promise<void> {
    accessKey = storage.clone(accessKey);

    if (!accessKey || !accessKey.id) {
      return q.reject(storage.storageError(storage.ErrorCode.NotFound));
    }

    return q(
      this.pool.query(
        `
    SELECT id
    FROM access_keys
    WHERE id = $1 AND account_id = $2
    `,
        [accessKey.id, accountId]
      )
    )
      .then((result: any) => {
        if (result.rows.length === 0) {
          return q.reject(storage.storageError(storage.ErrorCode.NotFound));
        }
        const fields: string[] = [];
        const values: any[] = [];
        let index = 1;

        if (accessKey.name !== undefined) {
          fields.push(`name = $${index++}`);
          values.push(accessKey.name);
        }

        if (accessKey.expires !== undefined) {
          fields.push(`expires = $${index++}`);
          values.push(accessKey.expires);
        }

        if (accessKey.friendlyName !== undefined) {
          fields.push(`friendly_name = $${index++}`);
          values.push(accessKey.friendlyName);
        }

        if (accessKey.description !== undefined) {
          fields.push(`description = $${index++}`);
          values.push(accessKey.description);
        }

        if (fields.length === 0) {
          return q.resolve();
        }

        values.push(accessKey.id, accountId);

        return q(
          this.pool.query(
            `
          UPDATE access_keys
          SET ${fields.join(", ")}
          WHERE id = $${index++} AND account_id = $${index}
          `,
            values
          )
        );
      })
      .then(() => undefined);
  }
  dropAll(): q.Promise<void> {
    const promises: q.Promise<void>[] = [];

    if (this.s3Client && this.bucketName) {
      const deleteAllBlobs = q(this.s3Client.send(new ListObjectsV2Command({ Bucket: this.bucketName })))
        .then((listResult: any) => {
          if (!listResult.Contents || listResult.Contents.length === 0) {
            return;
          }

          const objectsToDelete = listResult.Contents.map((obj: any) => ({ Key: obj.Key }));

          return q(
            this.s3Client.send(
              new DeleteObjectsCommand({
                Bucket: this.bucketName,
                Delete: { Objects: objectsToDelete },
              })
            )
          );
        })
        .then(() => undefined);

      promises.push(deleteAllBlobs);
    }

    return q.all(promises).then(() => undefined);
  }

  private mapAccount(row: any): storage.Account {
    return {
      id: row.id,
      email: row.email,
      name: row.name,
      azureAdId: row.azure_ad_id ?? undefined,
      gitHubId: row.github_id ?? undefined,
      microsoftId: row.microsoft_id ?? undefined,
      createdTime: Number(row.created_time),
    };
  }
  private mapApp(row: any): storage.App {
    return {
      id: row.id,
      name: row.name,
      createdTime: Number(row.created_time),
    };
  }
  private mapCollaboratorProperties(row: any): storage.CollaboratorProperties {
    return {
      accountId: row.account_id,
      permission: row.permission,
    };
  }
  private mapCollaboratorMap(rows: any[]): storage.CollaboratorMap {
    const map: storage.CollaboratorMap = {};

    rows.forEach((row: any) => {
      map[row.email] = this.mapCollaboratorProperties(row);
    });

    return map;
  }
  private mapDeployment(row: any): storage.Deployment {
    return {
      id: row.id,
      name: row.name,
      key: row.key,
      createdTime: Number(row.created_time),
    };
  }
  private mapAccessKey(row: any): storage.AccessKey {
    return {
      id: row.id,
      name: row.name,
      expires: row.expires ? Number(row.expires) : undefined,
      createdTime: Number(row.created_time),
      createdBy: row.created_by,
      friendlyName: row.friendly_name,
      description: row.description,
    };
  }
  private mapPackage(row: any): storage.Package {
    return {
      appVersion: row.app_version,
      blobUrl: row.blob_url,
      description: row.description,
      isDisabled: row.is_disabled,
      isMandatory: row.is_mandatory,
      label: row.label,
      manifestBlobUrl: row.manifest_blob_url,
      originalDeployment: row.original_deployment,
      originalLabel: row.original_label,
      packageHash: row.package_hash,
      releasedBy: row.released_by,
      releaseMethod: row.release_method,
      rollout: row.rollout,
      size: row.size,
      uploadTime: Number(row.upload_time),
    };
  }

  private addIsCurrentAccountProperty(app: storage.App, accountId: string): void {
    if (app && app.collaborators) {
      Object.keys(app.collaborators).forEach((email: string) => {
        if (app.collaborators[email].accountId === accountId) {
          app.collaborators[email].isCurrentAccount = true;
        }
      });
    }
  }
  private removeIsCurrentAccountProperty(app: storage.App): void {
    if (app && app.collaborators) {
      Object.keys(app.collaborators).forEach((email: string) => {
        if (app.collaborators[email].isCurrentAccount) {
          delete app.collaborators[email].isCurrentAccount;
        }
      });
    }
  }
  private isOwner(list: storage.CollaboratorMap, email: string): boolean {
    return list && list[email] && list[email].permission === storage.Permissions.Owner;
  }
}

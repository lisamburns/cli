package isolated

import (
	"code.cloudfoundry.org/cli/integration/helpers"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gbytes"
	. "github.com/onsi/gomega/gexec"
)

var _ = Describe("update-space-quota command", func() {
	var (
		quotaName string
	)

	BeforeEach(func() {
		quotaName = helpers.QuotaName()
	})

	Describe("help", func() {
		When("--help flag is set", func() {
			It("Displays command usage to output", func() {
				session := helpers.CF("update-space-quota", "--help")
				Eventually(session).Should(Say("NAME:"))
				Eventually(session).Should(Say("update-space-quota - Update an existing space quota"))
				Eventually(session).Should(Say("USAGE:"))
				Eventually(session).Should(Say(`cf update-space-quota QUOTA \[-m TOTAL_MEMORY\] \[-i INSTANCE_MEMORY\] \[-n NEW_NAME\] \[-r ROUTES\] \[-s SERVICE_INSTANCES\] \[-a APP_INSTANCES\] \[--allow-paid-service-plans | --disallow-paid-service-plans\] \[--allow-paid-service-plans\] \[--reserved-route-ports RESERVED_ROUTE_PORTS\]`))
				Eventually(session).Should(Say("OPTIONS:"))
				Eventually(session).Should(Say(`-a\s+Total number of application instances. -1 represents an unlimited amount.`))
				Eventually(session).Should(Say(`--allow-paid-service-plans\s+Allow provisioning instances of paid service plans.`))
				Eventually(session).Should(Say(`--disallow-paid-service-plans\s+Disallow provisioning instances of paid service plans.`))
				Eventually(session).Should(Say(`-i\s+Maximum amount of memory a process can have \(e.g. 1024M, 1G, 10G\). -1 represents an unlimited amount.`))
				Eventually(session).Should(Say(`-m\s+Total amount of memory all processes can have \(e.g. 1024M, 1G, 10G\).  -1 represents an unlimited amount.`))
				Eventually(session).Should(Say(`-n\s+New name`))
				Eventually(session).Should(Say(`-r\s+Total number of routes. -1 represents an unlimited amount.`))
				Eventually(session).Should(Say(`--reserved-route-ports\s+Maximum number of routes that may be created with ports. -1 represents an unlimited amount.`))
				Eventually(session).Should(Say(`-s\s+Total number of service instances. -1 represents an unlimited amount.`))
				Eventually(session).Should(Say("SEE ALSO:"))
				Eventually(session).Should(Say("space, space-quota, space-quotas"))
				Eventually(session).Should(Exit(0))
			})
		})
	})

	When("the environment is not setup correctly", func() {
		It("fails with the appropriate errors", func() {
			helpers.CheckEnvironmentTargetedCorrectly(true, false, ReadOnlyOrg, "space-quota", quotaName)
		})
	})

	When("the environment is set up correctly", func() {
		var (
			orgName   string
			spaceName string
			userName  string
		)

		BeforeEach(func() {
			orgName = helpers.NewOrgName()
			spaceName = helpers.NewSpaceName()
			helpers.SetupCF(orgName, spaceName)
			userName, _ = helpers.GetCredentials()

			totalMemory := "24M"
			instanceMemory := "6M"
			routes := "8"
			serviceInstances := "2"
			appInstances := "3"
			reservedRoutePorts := "1"
			session := helpers.CF("create-space-quota", quotaName, "-m", totalMemory, "-i", instanceMemory, "-r", routes, "-s", serviceInstances, "-a", appInstances, "--allow-paid-service-plans", "--reserved-route-ports", reservedRoutePorts)
			Eventually(session).Should(Exit(0))
		})

		AfterEach(func() {
			helpers.QuickDeleteOrg(orgName)
		})

		It("updates a space quota", func() {
			totalMemory := "25M"
			instanceMemory := "5M"
			serviceInstances := "1"
			appInstances := "2"
			reservedRoutePorts := "0"
			session := helpers.CF("update-space-quota", quotaName, "-m", totalMemory, "-i", instanceMemory, "-s", serviceInstances, "-a", appInstances, "--allow-paid-service-plans", "--reserved-route-ports", reservedRoutePorts)
			Eventually(session).Should(Say("Updating space quota %s as %s...", quotaName, userName))
			Eventually(session).Should(Say("OK"))
			Eventually(session).Should(Exit(0))

			session = helpers.CF("space-quota", quotaName)
			Eventually(session).Should(Say(`total memory:\s+%s`, totalMemory))
			Eventually(session).Should(Say(`instance memory:\s+%s`, instanceMemory))
			Eventually(session).Should(Say(`routes:\s+%s`, "8"))
			Eventually(session).Should(Say(`service instances:\s+%s`, serviceInstances))
			Eventually(session).Should(Say(`paid service plans:\s+%s`, "allowed"))
			Eventually(session).Should(Say(`app instances:\s+%s`, appInstances))
			Eventually(session).Should(Say(`route ports:\s+%s`, reservedRoutePorts))
			Eventually(session).Should(Exit(0))
		})

		When("the named quota does not exist", func() {
			It("displays a missing quota error message and fails", func() {
				session := helpers.CF("update-space-quota", "bogota")
				Eventually(session).Should(Say(`Updating space quota bogota as %s\.\.\.`, userName))
				Eventually(session).Should(Say("FAILED"))
				Eventually(session.Err).Should(Say("Space quota with name '%s' not found.", "bogota"))
				Eventually(session).Should(Exit(1))
			})
		})

		When("conflicting flags are specified", func() {
			It("displays a flag conflict error message and fails", func() {
				session := helpers.CF("update-space-quota", quotaName, "--allow-paid-service-plans", "--disallow-paid-service-plans")
				Eventually(session).Should(Say("FAILED"))
				Eventually(session.Err).Should(Say(`Incorrect Usage: The following arguments cannot be used together: --allow-paid-service-plans, --disallow-paid-service-plans`))
				Eventually(session).Should(Exit(1))
			})
		})
	})
})

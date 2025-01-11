package tests

import (
	"context"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/redis/go-redis/v9"
	"github.com/smarter-day/redstream"
	"github.com/smarter-day/redstream/mocks"
	"go.uber.org/mock/gomock"
)

func publishSomething(ctx context.Context, r redstream.IRedStream, data map[string]any) (string, error) {
	return r.Publish(ctx, data)
}

var _ = Describe("Publish with a mock UniversalClient", func() {
	var (
		mockCtrl        *gomock.Controller
		rs              redstream.IRedStream
		mockRedisClient *mocks.MockUniversalClient

		ctx    context.Context
		cancel context.CancelFunc
	)

	BeforeEach(func() {
		mockCtrl = gomock.NewController(GinkgoT())
		mockRedisClient = mocks.NewMockUniversalClient(mockCtrl)
		ctx, cancel = context.WithCancel(context.Background())

		// Provide a config that calls XGroupCreateMkStream("testStream","testGroup","0"):
		cfg := redstream.Config{
			StreamName:       "testStream",
			GroupName:        "testGroup",
			ConsumerName:     "testConsumer",
			LockExpiryStr:    "10s",
			LockExtendStr:    "5s",
			BlockDurationStr: "5s",
			EnableReclaim:    false,
			// We want to test normal publish, so duplicates= false
			DropConcurrentDuplicates: false,
		}

		// Creates *redisStream with a MockUniversalClient
		rs = NewRedStream(mockRedisClient, cfg)
	})

	AfterEach(func() {
		cancel()
		mockCtrl.Finish()
	})

	When("Publishing with DropConcurrentDuplicates=false", func() {
		It("should call XAdd and return the generated ID", func() {
			mockRedisClient.
				EXPECT().
				XGroupCreateMkStream(
					gomock.Any(),
					gomock.Any(),
					gomock.Any(),
					gomock.Any(),
					//"testStream",
					//"testGroup",
					//0,
				).
				Return(gomock.Any()).
				AnyTimes()

			// Now set the XAdd expectation
			mockRedisClient.
				EXPECT().
				XAdd(gomock.Any(), gomock.Any()).
				DoAndReturn(func(_ context.Context, args *redis.XAddArgs) (string, error) {
					Expect(args.Stream).To(Equal("testStream"))
					return "123-456", nil
				}).
				Times(1)

			msgID, err := publishSomething(ctx, rs, map[string]any{"foo": "bar"})
			Expect(err).NotTo(HaveOccurred())
			Expect(msgID).To(Equal("123-456"))
		})
	})
})

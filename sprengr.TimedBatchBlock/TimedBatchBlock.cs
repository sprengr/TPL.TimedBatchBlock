using System;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace sprengr.TimedBatchBlock
{
    public class TimedBatchBlock<T> : IPropagatorBlock<T, T[]>
    {
        private readonly ISourceBlock<T[]> _batchBlock;
        private readonly ITargetBlock<T> _timeoutBlock;
        private readonly Timer _triggerBatchTimer;

        public TimedBatchBlock(int batchSize, int triggerDueTimeInMs = 5000)
        {
            var batchBlock = new BatchBlock<T>(batchSize);
            _triggerBatchTimer = new Timer((state) => batchBlock.TriggerBatch());
            var timeoutBlock = new TransformBlock<T, T>((value) =>
            {
                _triggerBatchTimer.Change(triggerDueTimeInMs, Timeout.Infinite);
                return value;
            });

            timeoutBlock.LinkTo(batchBlock);

            _batchBlock = batchBlock;
            _timeoutBlock = timeoutBlock;
        }

        public Task Completion => _batchBlock.Completion;

        public void Complete()
        {
            _timeoutBlock.Complete();
            _batchBlock.Complete();
        }

        public T[] ConsumeMessage(DataflowMessageHeader messageHeader, ITargetBlock<T[]> targets, out bool messageConsumed)
        {
            return _batchBlock.ConsumeMessage(messageHeader, targets, out messageConsumed);
        }

        public void Fault(Exception exception)
        {
            _batchBlock.Fault(exception);
        }

        public IDisposable LinkTo(ITargetBlock<T[]> target, DataflowLinkOptions linkOptions)
        {
            return _batchBlock.LinkTo(target, linkOptions);
        }

        public DataflowMessageStatus OfferMessage(DataflowMessageHeader messageHeader, T messageValue, ISourceBlock<T> source, bool consumeToAccept)
        {
            return _timeoutBlock.OfferMessage(messageHeader, messageValue, source, consumeToAccept);
        }

        public void ReleaseReservation(DataflowMessageHeader messageHeader, ITargetBlock<T[]> target)
        {
            _batchBlock.ReleaseReservation(messageHeader, target);
        }

        public bool ReserveMessage(DataflowMessageHeader messageHeader, ITargetBlock<T[]> target)
        {
            return _batchBlock.ReserveMessage(messageHeader, target);
        }
    }
}

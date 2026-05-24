// 时间范围选择器组件
import { Select, DatePicker, Space } from 'antd';
import { useState } from 'react';
import dayjs, { Dayjs } from 'dayjs';
import { TIME_RANGE_OPTIONS } from '../../utils/constants';
import type { RangePickerProps } from 'antd/es/date-picker';

const { RangePicker } = DatePicker;

interface TimeRangeSelectorProps {
  onChange: (range: { start: string; end: string }) => void;
  defaultValue?: string;
}

export default function TimeRangeSelector({ onChange, defaultValue = '24h' }: TimeRangeSelectorProps) {
  const [selectedRange, setSelectedRange] = useState(defaultValue);
  const [customRange, setCustomRange] = useState<[Dayjs, Dayjs] | null>(null);

  const handleRangeChange = (value: string) => {
    setSelectedRange(value);

    if (value !== 'custom') {
      const option = TIME_RANGE_OPTIONS.find(opt => opt.value === value);
      if (option) {
        const end = dayjs();
        const start = end.subtract(option.hours, 'hour');
        onChange({
          start: start.format('YYYY-MM-DD HH:mm:ss'),
          end: end.format('YYYY-MM-DD HH:mm:ss')
        });
      }
    }
  };

  const handleCustomRangeChange: RangePickerProps['onChange'] = (dates) => {
    if (dates?.[0] && dates[1]) {
      setCustomRange([dates[0], dates[1]]);
      onChange({
        start: dates[0].format('YYYY-MM-DD HH:mm:ss'),
        end: dates[1].format('YYYY-MM-DD HH:mm:ss')
      });
    } else {
      setCustomRange(null);
    }
  };

  return (
    <Space>
      <Select
        value={selectedRange}
        onChange={handleRangeChange}
        style={{ width: 150 }}
        options={TIME_RANGE_OPTIONS}
      />
      {selectedRange === 'custom' && (
        <RangePicker
          showTime
          value={customRange}
          onChange={handleCustomRangeChange}
          format="YYYY-MM-DD HH:mm"
        />
      )}
    </Space>
  );
}

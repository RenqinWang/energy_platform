#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
运行建议规则引擎
定义规则和规则匹配逻辑
"""

import json
from typing import Dict, List, Any, Optional
from datetime import datetime


class Rule:
    """规则类"""

    def __init__(self, rule_id: str, rule_name: str, advice_type: str,
                 risk_level: str, advice_template: str, condition_func):
        """
        初始化规则

        Args:
            rule_id: 规则ID
            rule_name: 规则名称
            advice_type: 建议类型 (load_change, anomaly, efficiency, economic)
            risk_level: 风险等级 (low, medium, high)
            advice_template: 建议模板
            condition_func: 条件判断函数
        """
        self.rule_id = rule_id
        self.rule_name = rule_name
        self.advice_type = advice_type
        self.risk_level = risk_level
        self.advice_template = advice_template
        self.condition_func = condition_func

    def check(self, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        检查规则是否触发

        Args:
            data: 数据字典

        Returns:
            如果触发，返回建议字典；否则返回None
        """
        result = self.condition_func(data)

        if result['triggered']:
            advice = {
                'rule_id': self.rule_id,
                'rule_name': self.rule_name,
                'advice_type': self.advice_type,
                'risk_level': self.risk_level,
                'advice_text': self.advice_template.format(**result['evidence']),
                'evidence_metrics': json.dumps(result['evidence'], ensure_ascii=False)
            }
            return advice

        return None


class RuleEngine:
    """规则引擎"""

    def __init__(self):
        """初始化规则引擎"""
        self.rules = []
        self._init_rules()

    def _init_rules(self):
        """初始化所有规则"""

        # 规则1: 负荷下降建议
        def rule_load_decrease(data):
            forecast = data.get('forecast', {})
            current = data.get('current', {})

            predicted_avg = forecast.get('avg_cooling_6h', 0)
            current_avg = current.get('avg_cooling_6h', 0)
            running_units = current.get('running_units', 0)

            if current_avg > 0 and predicted_avg < current_avg * 0.7 and running_units >= 2:
                decrease_pct = (1 - predicted_avg / current_avg) * 100
                return {
                    'triggered': True,
                    'evidence': {
                        'predicted_cooling_kwh': round(predicted_avg, 2),
                        'current_cooling_kwh': round(current_avg, 2),
                        'decrease_pct': round(decrease_pct, 1),
                        'running_units': running_units
                    }
                }
            return {'triggered': False}

        self.rules.append(Rule(
            rule_id='R001',
            rule_name='负荷下降建议',
            advice_type='load_change',
            risk_level='medium',
            advice_template='预测未来6小时负荷下降{decrease_pct}%（当前{current_cooling_kwh}kWh → 预测{predicted_cooling_kwh}kWh），当前运行{running_units}台冷机，建议关停1台以提高能效',
            condition_func=rule_load_decrease
        ))

        # 规则2: 负荷上升建议
        def rule_load_increase(data):
            forecast = data.get('forecast', {})
            current = data.get('current', {})

            predicted_avg = forecast.get('avg_cooling_6h', 0)
            current_avg = current.get('avg_cooling_6h', 0)
            running_units = current.get('running_units', 0)

            if current_avg > 0 and predicted_avg > current_avg * 1.3 and running_units < 3:
                increase_pct = (predicted_avg / current_avg - 1) * 100
                return {
                    'triggered': True,
                    'evidence': {
                        'predicted_cooling_kwh': round(predicted_avg, 2),
                        'current_cooling_kwh': round(current_avg, 2),
                        'increase_pct': round(increase_pct, 1),
                        'running_units': running_units
                    }
                }
            return {'triggered': False}

        self.rules.append(Rule(
            rule_id='R002',
            rule_name='负荷上升建议',
            advice_type='load_change',
            risk_level='high',
            advice_template='预测未来6小时负荷上升{increase_pct}%（当前{current_cooling_kwh}kWh → 预测{predicted_cooling_kwh}kWh），当前运行{running_units}台冷机，建议提前启动备用冷机',
            condition_func=rule_load_increase
        ))

        # 规则3: 温差异常建议
        def rule_temp_diff_anomaly(data):
            current = data.get('current', {})

            supply_temp = current.get('avg_supply_temp')
            return_temp = current.get('avg_return_temp')
            run_flag = current.get('run_flag', 0)

            if supply_temp is not None and return_temp is not None and run_flag > 0:
                temp_diff = return_temp - supply_temp

                if temp_diff < 3.0 or temp_diff > 8.0:
                    return {
                        'triggered': True,
                        'evidence': {
                            'supply_temp': round(supply_temp, 2),
                            'return_temp': round(return_temp, 2),
                            'temp_diff': round(temp_diff, 2),
                            'normal_range': '3-8℃'
                        }
                    }
            return {'triggered': False}

        self.rules.append(Rule(
            rule_id='R003',
            rule_name='温差异常建议',
            advice_type='anomaly',
            risk_level='high',
            advice_template='冷机温差异常（供水{supply_temp}℃，回水{return_temp}℃，温差{temp_diff}℃，正常范围{normal_range}），可能存在传感器故障或设备问题',
            condition_func=rule_temp_diff_anomaly
        ))

        # 规则4: 能效低下建议
        def rule_low_efficiency(data):
            current = data.get('current', {})

            avg_cop = current.get('avg_cop')
            operation_rate = current.get('operation_rate', 0)

            if avg_cop is not None and avg_cop < 2.5 and operation_rate > 50:
                return {
                    'triggered': True,
                    'evidence': {
                        'avg_cop': round(avg_cop, 2),
                        'operation_rate': round(operation_rate, 1),
                        'threshold_cop': 2.5
                    }
                }
            return {'triggered': False}

        self.rules.append(Rule(
            rule_id='R004',
            rule_name='能效低下建议',
            advice_type='efficiency',
            risk_level='medium',
            advice_template='冷机能效比{avg_cop}低于正常水平（阈值{threshold_cop}），运行率{operation_rate}%，建议检查设备运行状态或考虑维护',
            condition_func=rule_low_efficiency
        ))

        # 规则5: 频繁启停建议
        def rule_frequent_start(data):
            current = data.get('current', {})

            start_count_24h = current.get('start_count_24h', 0)

            if start_count_24h > 10:
                return {
                    'triggered': True,
                    'evidence': {
                        'start_count_24h': int(start_count_24h),
                        'threshold': 10
                    }
                }
            return {'triggered': False}

        self.rules.append(Rule(
            rule_id='R005',
            rule_name='频繁启停建议',
            advice_type='efficiency',
            risk_level='medium',
            advice_template='冷机频繁启停（24小时内启动{start_count_24h}次，超过阈值{threshold}次），可能影响设备寿命，建议检查控制策略',
            condition_func=rule_frequent_start
        ))

        # 规则6: 长时间满负荷建议
        def rule_high_load_duration(data):
            current = data.get('current', {})

            operation_rate = current.get('operation_rate', 0)
            high_load_hours = current.get('high_load_hours', 0)

            if operation_rate > 95 and high_load_hours > 6:
                return {
                    'triggered': True,
                    'evidence': {
                        'operation_rate': round(operation_rate, 1),
                        'high_load_hours': round(high_load_hours, 1),
                        'threshold_rate': 95,
                        'threshold_hours': 6
                    }
                }
            return {'triggered': False}

        self.rules.append(Rule(
            rule_id='R006',
            rule_name='长时间满负荷建议',
            advice_type='efficiency',
            risk_level='high',
            advice_template='冷机长时间满负荷运行（运行率{operation_rate}%，持续{high_load_hours}小时），建议启动备用机组分担负荷',
            condition_func=rule_high_load_duration
        ))

    def apply_rules(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        应用所有规则

        Args:
            data: 数据字典

        Returns:
            触发的建议列表
        """
        advices = []

        for rule in self.rules:
            try:
                advice = rule.check(data)
                if advice:
                    advices.append(advice)
            except Exception as e:
                print(f"规则 {rule.rule_id} 执行失败: {e}")

        return advices

    def get_rule_count(self) -> int:
        """获取规则数量"""
        return len(self.rules)

    def get_rule_summary(self) -> List[Dict[str, str]]:
        """获取规则摘要"""
        return [
            {
                'rule_id': rule.rule_id,
                'rule_name': rule.rule_name,
                'advice_type': rule.advice_type,
                'risk_level': rule.risk_level
            }
            for rule in self.rules
        ]


if __name__ == "__main__":
    # 测试规则引擎
    print("规则引擎测试")
    print("="*60)

    engine = RuleEngine()
    print(f"已加载 {engine.get_rule_count()} 条规则:\n")

    for rule_info in engine.get_rule_summary():
        print(f"  [{rule_info['rule_id']}] {rule_info['rule_name']}")
        print(f"      类型: {rule_info['advice_type']}, 风险: {rule_info['risk_level']}")

    # 测试数据
    print("\n" + "="*60)
    print("测试规则触发")
    print("="*60)

    test_data = {
        'current': {
            'avg_cooling_6h': 1200.0,
            'running_units': 2,
            'avg_supply_temp': 7.5,
            'avg_return_temp': 12.5,
            'run_flag': 1,
            'avg_cop': 2.2,
            'operation_rate': 85.0,
            'start_count_24h': 12,
            'high_load_hours': 8
        },
        'forecast': {
            'avg_cooling_6h': 800.0
        }
    }

    advices = engine.apply_rules(test_data)

    print(f"\n触发了 {len(advices)} 条建议:\n")
    for i, advice in enumerate(advices, 1):
        print(f"{i}. [{advice['rule_id']}] {advice['rule_name']}")
        print(f"   类型: {advice['advice_type']}, 风险: {advice['risk_level']}")
        print(f"   建议: {advice['advice_text']}")
        print(f"   证据: {advice['evidence_metrics']}")
        print()

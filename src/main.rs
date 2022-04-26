use anyhow::Context;
use csv::Trim;
use fxhash::{FxHashMap, FxHashSet};
use serde::de::{Error, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::hash_map::Entry;
use std::fmt::Formatter;
use std::ops::SubAssign;
use std::str::FromStr;
use std::{env, fmt, fs, io};

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Deserialize, Serialize)]
struct AccountId(u16);

impl fmt::Display for AccountId {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Deserialize)]
struct TransactionId(u16);

impl fmt::Display for TransactionId {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, PartialOrd)]
struct FixedPoint(i64);

impl FixedPoint {
    #[must_use]
    fn checked_add(&self, rhs: FixedPoint) -> Option<FixedPoint> {
        self.0.checked_add(rhs.0).map(FixedPoint)
    }

    #[must_use]
    fn checked_sub(&self, rhs: FixedPoint) -> Option<FixedPoint> {
        self.0.checked_sub(rhs.0).map(FixedPoint)
    }
}

impl From<i64> for FixedPoint {
    fn from(val: i64) -> Self {
        FixedPoint(val)
    }
}

impl SubAssign<FixedPoint> for FixedPoint {
    fn sub_assign(&mut self, rhs: FixedPoint) {
        self.0.sub_assign(rhs.0)
    }
}

impl FromStr for FixedPoint {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (decimal_part, frac_part) = value.split_once('.').unwrap_or((value, ""));
        let decimal: i64 = if decimal_part.is_empty() {
            0
        } else {
            decimal_part.parse()?
        };
        let frac = if frac_part.is_empty() {
            0
        } else {
            // description is not clear about this but below code assumes that as long
            // as first 4 chars create valid numbers we are ok.
            // in real prod env though we should only accept the input if that's the case
            // and not ignore what's on right as it means that something is not behaving well in the stack
            let len = frac_part.len().min(4);
            let parsed: i64 = if len < 4 { frac_part } else { &frac_part[0..4] }.parse()?;
            let shifts = 4 - len;
            parsed * 10i64.pow(shifts.try_into().expect("shifts fits u32"))
        };
        let shifted_decimal = decimal.checked_mul(10000).context("TooLargeNumber")?;
        Ok(FixedPoint(frac + shifted_decimal))
    }
}

impl<'de> Deserialize<'de> for FixedPoint {
    fn deserialize<D>(deserializer: D) -> Result<FixedPoint, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct StrVisitor;
        impl<'de> Visitor<'de> for StrVisitor {
            type Value = FixedPoint;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a str")
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(FixedPoint::from_str(v).unwrap())
            }
        }
        deserializer.deserialize_str(StrVisitor)
    }
}

impl Serialize for FixedPoint {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.collect_str(&format_args!("{}", self))
    }
}

impl fmt::Display for FixedPoint {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let dec_part = self.0 / 10000;
        let frac_part = self.0 % 10000;
        write!(f, "{}.{}", dec_part, frac_part,)?;
        Ok(())
    }
}

#[derive(PartialEq)]
enum DepositStatus {
    Rejected,
    None,
    Disputed,
    Resolved,
}

#[derive(Clone, Default)]
struct Account {
    frozen: bool,
    available_funds: FixedPoint,
    locked_funds: FixedPoint,
}

impl Account {
    fn deposit(
        &mut self,
        account_id: AccountId,
        transaction_id: TransactionId,
        amount: FixedPoint,
    ) -> anyhow::Result<()> {
        self.available_funds = self.available_funds.checked_add(amount).ok_or_else(|| {
            anyhow::format_err!(
                "Deposit TransactionId={transaction_id} would overflow Account={account_id}"
            )
        })?;
        Ok(())
    }

    fn withdraw(
        &mut self,
        account_id: AccountId,
        transaction_id: TransactionId,
        amount: FixedPoint,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.available_funds >= amount,
            "Withdrawal TransactionId={transaction_id} failed, insufficient Account={account_id} balance"
        );
        self.available_funds -= amount;
        Ok(())
    }

    fn freeze_funds(
        &mut self,
        account_id: AccountId,
        transaction_id: TransactionId,
        amount: FixedPoint,
    ) -> anyhow::Result<()> {
        // we are okay with the funds going negative in this case, but we are not ok with underflow.
        let available_funds = self.available_funds.checked_sub(amount).ok_or_else(
            || anyhow::format_err!("Freezing funds TransactionId={transaction_id} would underflow available funds Account={account_id}")
        )?;
        let locked_funds = self.locked_funds.checked_add(amount).ok_or_else(
            || anyhow::format_err!("Freezing funds TransactionId={transaction_id} would overflow locked funds Account={account_id}")
        )?;
        self.available_funds = available_funds;
        self.locked_funds = locked_funds;
        Ok(())
    }

    fn release_funds(
        &mut self,
        account_id: AccountId,
        transaction_id: TransactionId,
        amount: FixedPoint,
    ) -> anyhow::Result<()> {
        let available_funds = self.available_funds.checked_add(amount).ok_or_else(
            || anyhow::format_err!("Releasing funds TransactionId={transaction_id} would overflow available funds Account={account_id}")
        )?;
        let locked_funds = self.locked_funds.checked_sub(amount).ok_or_else(
            || anyhow::format_err!("Releasing funds TransactionId={transaction_id} would underflow locked funds Account={account_id}")
        )?;
        self.available_funds = available_funds;
        self.locked_funds = locked_funds;
        Ok(())
    }

    fn chargeback_funds(
        &mut self,
        account_id: AccountId,
        transaction_id: TransactionId,
        amount: FixedPoint,
    ) -> anyhow::Result<()> {
        let locked_funds = self.locked_funds.checked_sub(amount).ok_or_else(
            || anyhow::format_err!("Chargeback TransactionId={transaction_id} would underflow locked funds Account={account_id}")
        )?;
        self.locked_funds = locked_funds;
        self.frozen = true;
        Ok(())
    }
}

struct Deposit {
    account_id: AccountId,
    status: DepositStatus,
    amount: FixedPoint,
}

#[derive(Default)]
struct ToyEngine {
    accounts: FxHashMap<AccountId, Account>,
    deposits: FxHashMap<TransactionId, Deposit>,
    // only exists to make sure we don't double process same tx id
    withdrawals: FxHashSet<TransactionId>,
}

impl ToyEngine {
    fn process_deposit(
        &mut self,
        transaction_id: TransactionId,
        account_id: AccountId,
        amount: FixedPoint,
    ) -> anyhow::Result<()> {
        if self.withdrawals.contains(&transaction_id) {
            anyhow::bail!("TransactionId={transaction_id} already used");
        }
        let deposit = match self.deposits.entry(transaction_id) {
            Entry::Occupied(_) => {
                anyhow::bail!("TransactionId={transaction_id} already used");
            }
            Entry::Vacant(e) => e.insert(Deposit {
                account_id,
                status: DepositStatus::None,
                amount,
            }),
        };
        let account = self
            .accounts
            .entry(account_id)
            .or_insert_with(Account::default);
        if account.frozen {
            anyhow::bail!("Deposit attempted on frozen account={account_id}");
        }
        if let Err(e) = account.deposit(account_id, transaction_id, amount) {
            deposit.status = DepositStatus::Rejected;
            return Err(e);
        }
        Ok(())
    }

    fn process_withdrawal(
        &mut self,
        transaction_id: TransactionId,
        account_id: AccountId,
        amount: FixedPoint,
    ) -> anyhow::Result<()> {
        if self.deposits.contains_key(&transaction_id) || !self.withdrawals.insert(transaction_id) {
            anyhow::bail!("TransactionId={transaction_id} already used");
        }
        let account = self.accounts.get_mut(&account_id).ok_or_else(|| {
            anyhow::format_err!("Withdrawal attempt on invalid account={account_id}")
        })?;
        if account.frozen {
            anyhow::bail!("Withdrawal attempted on frozen account={account_id}");
        }
        account.withdraw(account_id, transaction_id, amount)
    }

    fn process_dispute(
        &mut self,
        transaction_id: TransactionId,
        account_id: AccountId,
    ) -> anyhow::Result<()> {
        let mut deposit = self.deposits.get_mut(&transaction_id).ok_or_else(|| {
            anyhow::format_err!("Dispute targets non existing TransactionId={transaction_id}")
        })?;
        anyhow::ensure!(
            account_id == deposit.account_id,
            "Dispute carries unexpected AccountId={}",
            deposit.account_id,
        );
        anyhow::ensure!(
            deposit.status == DepositStatus::None,
            "Dispute TransactionId={transaction_id} targets unexpected deposit"
        );
        let account = self
            .accounts
            .get_mut(&account_id)
            .expect("Account exists at this dispute point");
        account.freeze_funds(account_id, transaction_id, deposit.amount)?;
        deposit.status = DepositStatus::Disputed;
        Ok(())
    }

    fn process_resolve(
        &mut self,
        transaction_id: TransactionId,
        account_id: AccountId,
    ) -> anyhow::Result<()> {
        let mut deposit = self.deposits.get_mut(&transaction_id).ok_or_else(|| {
            anyhow::format_err!("Resolve targets non existing TransactionId={transaction_id}")
        })?;
        anyhow::ensure!(
            account_id == deposit.account_id,
            "Resolve carries unexpected AccountId={}",
            deposit.account_id,
        );
        anyhow::ensure!(
            deposit.status == DepositStatus::Disputed,
            "Resolve TransactionId={transaction_id} targets non disputed deposit"
        );
        let account = self
            .accounts
            .get_mut(&account_id)
            .expect("Account exists at this dispute point");
        account.release_funds(account_id, transaction_id, deposit.amount)?;
        deposit.status = DepositStatus::Resolved;
        Ok(())
    }

    fn process_chargeback(
        &mut self,
        transaction_id: TransactionId,
        account_id: AccountId,
    ) -> anyhow::Result<()> {
        let mut deposit = self.deposits.get_mut(&transaction_id).ok_or_else(|| {
            anyhow::format_err!("Chargeback targets non existing TransactionId={transaction_id}")
        })?;
        anyhow::ensure!(
            account_id == deposit.account_id,
            "Chargeback carries unexpected AccountId={}",
            deposit.account_id,
        );
        anyhow::ensure!(
            deposit.status == DepositStatus::Disputed,
            "Chargeback TransactionId={transaction_id} targets non disputed deposit"
        );
        let account = self
            .accounts
            .get_mut(&account_id)
            .expect("Account exists at this dispute point");
        account.chargeback_funds(account_id, transaction_id, deposit.amount)?;
        deposit.status = DepositStatus::Resolved;
        Ok(())
    }
}

fn process<R: io::Read>(reader: R, engine: &mut ToyEngine) -> anyhow::Result<()> {
    #[derive(Deserialize)]
    #[serde(rename_all = "kebab-case")]
    enum TransactionType {
        Deposit,
        Withdrawal,
        Dispute,
        Resolve,
        Chargeback,
    }

    #[derive(Deserialize)]
    struct Record {
        #[serde(rename = "type")]
        type_: TransactionType,
        #[serde(rename = "client")]
        account_id: AccountId,
        #[serde(rename = "tx")]
        transaction_id: TransactionId,
        amount: FixedPoint,
    }

    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .comment(Some(b'#'))
        .trim(Trim::All)
        .from_reader(reader);
    for result in reader.deserialize() {
        let record = result;
        let record: Record = match record {
            Ok(record) => record,
            Err(_) => {
                // description was not clear what to do with bad lines,
                // we can either fail processing or ignore them
                // however since we will be ignoring bad withdrawal later on, let's ignore faulty lines as well.
                continue;
            }
        };
        let Record {
            type_,
            account_id,
            transaction_id,
            amount,
        } = record;
        // we ought to report the errors somewhere, but not in this exercise.
        let _ = match type_ {
            TransactionType::Deposit => engine.process_deposit(transaction_id, account_id, amount),
            TransactionType::Withdrawal => {
                engine.process_withdrawal(transaction_id, account_id, amount)
            }
            TransactionType::Dispute => engine.process_dispute(transaction_id, account_id),
            TransactionType::Resolve => engine.process_resolve(transaction_id, account_id),
            TransactionType::Chargeback => engine.process_chargeback(transaction_id, account_id),
        };
    }
    Ok(())
}

fn dump<'a, W: io::Write>(
    accounts: impl IntoIterator<Item = (&'a AccountId, &'a Account)>,
    output: W,
) -> anyhow::Result<()> {
    #[derive(Serialize)]
    struct Record {
        client: AccountId,
        available: FixedPoint,
        held: FixedPoint,
        total: FixedPoint,
        locked: bool,
    }
    let mut writer = csv::Writer::from_writer(output);
    for (account_id, account) in accounts {
        let total = if let Some(total) = account.available_funds.checked_add(account.locked_funds) {
            total
        } else {
            // Ignoring overflow here is.. not nice, but not necessarily wrong,
            // we would error the accounts we are unable to print, and make note of them,
            // in future revision of the code we would know what to do with those.
            continue;
        };
        let record = Record {
            client: *account_id,
            available: account.available_funds,
            held: account.locked_funds,
            total,
            locked: account.frozen,
        };
        writer.serialize(&record)?;
    }
    Ok(())
}

fn main() -> anyhow::Result<()> {
    let args: Vec<_> = env::args().collect();
    let path = args
        .get(1)
        .ok_or_else(|| anyhow::format_err!("Argument missing - path to a .csv file to process"))?;
    let file = fs::File::open(path)?;
    let file = io::BufReader::new(file);
    let mut engine = ToyEngine::default();
    process(file, &mut engine)?;
    dump(&engine.accounts, io::stdout())
}

#[cfg(test)]
mod tests {
    use crate::{dump, process, FixedPoint, ToyEngine};
    use std::collections::BTreeMap;
    use std::str::FromStr;

    #[test]
    fn decimal() {
        assert_eq!(FixedPoint::from_str("1.23456").unwrap(), 12345.into());
        assert_eq!(FixedPoint::from_str("1.234").unwrap(), 12340.into());
        assert_eq!(FixedPoint::from_str("1").unwrap(), 10000.into());
        assert_eq!(FixedPoint::from_str("1.").unwrap(), 10000.into());
        assert_eq!(FixedPoint::from_str("0.1234").unwrap(), 1234.into());
        assert_eq!(FixedPoint::from_str(".1234").unwrap(), 1234.into());
        assert_eq!(
            FixedPoint::from_str("123456789.123456").unwrap(),
            1234567891234.into()
        );

        assert_eq!(
            format!("{}", FixedPoint::from_str("1.234").unwrap()),
            "1.2340"
        );
        assert_eq!(
            format!("{}", FixedPoint::from_str("123456789.123456").unwrap()),
            "123456789.1234"
        );
    }
    #[test]
    fn convoluted_example() {
        let mut engine = ToyEngine::default();
        process(
            r#"
type,    client,  tx,       amount
# 1 -> deposits borked value -> empty account
deposit,      1,   1,   0.00001234
# 2 -> attempts to withdraw too much -> balance 2.0
deposit,      2,   2,          2.0
withdrawal,   2,   3,          3.0
# 3 -> withdrawal first -> account '3' will not get created
withdrawal,   3,   4,          1.0
# 4 -> ok dispute
deposit,      4,   5,          1.0
dispute,      4,   5,
# 5 -> ok dispute, resolve
deposit,      5,   6,          1.0
dispute,      5,   6,
resolve,      5,   6,
# 6 -> ok dispute, chargeback
deposit,      6,   7,          1.0
dispute,      6,   7,
chargeback,   6,   7,
# 7 -> disputed acc can withdraw deposit
deposit,      7,   8,          1.0
dispute,      7,   8,
deposit,      7,   9,          3.0
withdrawal,   7,  10,          2.0
# 8 -> resolved acc can withdraw deposit
deposit,      8,  11,          1.0
dispute,      8,  12,
resolve,      8,  12,
deposit,      8,  13,          3.0
withdrawal,   8,  14,          2.0
# 8 -> charged back acc can not withdraw nor deposit
deposit,      9,  15,         50.0
deposit,      9,  16,          1.0
dispute,      9,  16,
chargeback,   9,  16,
deposit,      9,  17,          3.0
withdrawal,   9,  18,          1.0
# 10 -> dispute carries wrong acc id
deposit,      10, 19,          1.0
dispute,       1, 19,
# 11 -> resolve carries wrong acc id
deposit,      11, 20,          1.0
dispute,      11, 20,
resolve,       1, 20,
# 11 -> chargeback carries wrong acc id
deposit,      12, 21,          1.0
dispute,      12, 21,
chargeback,    1, 21,
"#
            .trim()
            .as_bytes(),
            &mut engine,
        )
        .unwrap();
        let mut buff = Vec::new();
        let sorted_accounts: BTreeMap<_, _> = engine.accounts.clone().into_iter().collect();
        dump(&sorted_accounts, &mut buff).unwrap();
        assert_eq!(
            std::str::from_utf8(&buff).unwrap().trim(),
            r#"
client,available,held,total,locked
1,0.0,0.0,0.0,false
2,2.0,0.0,2.0,false
4,0.0,1.0,1.0,false
5,1.0,0.0,1.0,false
6,0.0,0.0,0.0,true
7,1.0,1.0,2.0,false
8,2.0,0.0,2.0,false
9,50.0,0.0,50.0,true
10,1.0,0.0,1.0,false
11,0.0,1.0,1.0,false
12,0.0,1.0,1.0,false
"#
            .trim(),
        )
    }
}

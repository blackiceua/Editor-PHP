<?php
/**
 * SQL Server driver for DataTables PHP libraries
 *
 *  @author    SpryMedia
 *  @copyright 2013 SpryMedia ( http://sprymedia.co.uk )
 *  @license   http://editor.datatables.net/license DataTables Editor
 *  @link      http://editor.datatables.net
 */

namespace DataTables\Database\Driver;
if (!defined('DATATABLES')) exit();

use PDO;
use DataTables\Database\Query;

/**
 * SQL Server driver for DataTables Database Query class
 *  @internal
 */
class SqlserverQuery extends Query {
	/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
	 * Private properties
	 */
	private $_stmt;


	protected $_identifier_limiter = array( '[', ']' );

	/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
	 * Public methods
	 */

	static function connect( $user, $pass='', $host='', $port='', $db='', $dsn='' )
	{
		if ( is_array( $user ) ) {
			$opts = $user;
			$user = $opts['user'];
			$pass = $opts['pass'];
			$port = $opts['port'];
			$host = $opts['host'];
			$db   = $opts['db'];
			$dsn  = isset( $opts['dsn'] ) ? $opts['dsn'] : '';
			$pdoAttr = isset( $opts['pdoAttr'] ) ? $opts['pdoAttr'] : array();
		}

		try {
			$pdoAttr[ PDO::ATTR_ERRMODE ] = PDO::ERRMODE_EXCEPTION;

			if ( in_array( 'sqlsrv', PDO::getAvailableDrivers() ) ) {
				// Windows
				if ( $port !== "" ) {
					$port = ",{$port}";
				}

				$pdo = new PDO(
					"sqlsrv:Server={$host}{$port};Database={$db}".self::dsnPostfix( $dsn ),
					$user,
					$pass,
					$pdoAttr
				);
			}
			else {
				// Linux
				if ( $port !== "" ) {
					$port = ":{$port}";
				}

				$pdo = new PDO(
					"dblib:host={$host}{$port};dbname={$db}".self::dsnPostfix( $dsn ),
					$user,
					$pass,
					$pdoAttr
				);
		}

		} catch (\PDOException $e) {
			// If we can't establish a DB connection then we return a DataTables
			// error.
			echo json_encode( array(
				"error" => "An error occurred while connecting to the database ".
					"'{$db}'. The error reported by the server was: ".$e->getMessage()
			) );
			exit(0);
		}

		return $pdo;
	}

  /**
   * Order by
   *  @param string|string[] $order Columns and direction to order by - can
   *    be specified as individual names, an array of names, a string of comma
   *    separated names or any combination of those.
   *  @return self
   */
  public function order ( $order )
  {
    if ( $order === null ) {
      return $this;
    }

    if ( !is_array($order) ) {
      $order = preg_split('/\,(?![^\(]*\))/',$order);
    }
    for ( $i=0 ; $i<count($order) ; $i++ ) {
      // Find fields with whitespace in name.
      preg_match('/(asc|desc)/', $order[$i], $directions);
      $direction = reset($directions);
      $order_field = str_replace(array(' asc', ' desc'), '', $order[$i]);

      if ($order_field && $direction && count(explode(' ', $order_field)) >= 2) {
        $identifier = preg_replace('/([\w\s]+)(\s+(asc|desc))/', '[$1] $2', $order);
        $this->_order[] = reset($identifier);
      }
      else {
        $direction = '';
        $this->_order[] = $this->_protect_identifiers( $order[$i] ).' '.$direction;
      }

    }
    return $this;
  }

	/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
	 * Protected methods
	 */

	protected function _prepare( $sql )
	{
		$this->database()->debugInfo( $sql, $this->_bindings );

		$resource = $this->database()->resource();
		$this->_stmt = $resource->prepare( $sql );

		// bind values
		for ( $i=0 ; $i<count($this->_bindings) ; $i++ ) {
			$binding = $this->_bindings[$i];

			$this->_stmt->bindValue(
				$binding['name'],
				$binding['value'],
				$binding['type'] ? $binding['type'] : \PDO::PARAM_STR
			);
		}
	}


	protected function _exec()
	{
		try {
			$this->_stmt->execute();
		}
		catch (PDOException $e) {
			throw new \Exception( "An SQL error occurred: ".$e->getMessage() );
			error_log( "An SQL error occurred: ".$e->getMessage() );
			return false;
		}

		$resource = $this->database()->resource();
		return new SqlserverResult( $resource, $this->_stmt );
	}

  /**
   * Protect field names
   * @param string $identifier String to be protected
   * @return string
   * @internal
   */
  protected function _protect_identifiers($identifier, $field_build = false )
  {
    $idl = $this->_identifier_limiter;

    // No escaping character
    if ( ! $idl ) {
      return $identifier;
    }

    $left = $idl[0];
    $right = $idl[1];

    // Dealing with a function or other expression? Just return immediately
    if (strpos($identifier, '(') !== FALSE || strpos($identifier, '*') !== FALSE || (strpos($identifier, ' ') !== FALSE && !$field_build))
    {
      return $identifier;
    }

    // Going to be operating on the spaces in strings, to simplify the white-space
    $identifier = preg_replace('/[\t ]+/', ' ', $identifier);

    // Find if our identifier has an alias, so we don't escape that
    if ( strpos($identifier, ' as ') !== false ) {
      $alias = strstr($identifier, ' as ');
      $identifier = substr($identifier, 0, - strlen($alias));
    }
    else {
      $alias = '';
    }

    $a = explode('.', $identifier);
    return $left . implode($right.'.'.$left, $a) . $right . $alias;
  }

  /**
   * Create a comma separated field list
   * @param bool $addAlias Flag to add an alias
   * @return string
   * @internal
   */
  protected function _build_field( $addAlias=false )
  {
    $a = array();
    $asAlias = $this->_supportsAsAlias ?
      ' as ' :
      ' ';
    $counter = count($this->_field);
    for ( $i=0 ; $i < $counter ; $i++ ) {
      $field = $this->_field[$i];

      // Keep the name when referring to a table
      if ( $addAlias && $field !== '*' && strpos($field, '(') === false ) {
        $split = preg_split( '/ as (?![^\(]*\))/i', $field );

        if ( count($split) > 1 ) {
          $a[] = $this->_protect_identifiers( $split[0], true ).$asAlias.
            $this->_field_quote. $split[1] .$this->_field_quote;
        }
        else {
          $a[] = $this->_protect_identifiers( $field, true ).$asAlias.
            $this->_field_quote. $field .$this->_field_quote;
        }
      }
      else if ( $addAlias && strpos($field, '(') !== false && ! strpos($field, ' as ') ) {
        $a[] = $this->_protect_identifiers( $field, true ).$asAlias.
          $this->_field_quote. $field .$this->_field_quote;
      }
      else {
        $a[] = $this->_protect_identifiers( $field, true );
      }
    }

    return ' '.implode(', ', $a).' ';
  }

  /**
   * {@inheritdoc}
   */
  protected function _build_where()
  {
    if ( count($this->_where) === 0 ) {
      return "";
    }

    $condition = "WHERE ";

    for ( $i=0 ; $i<count($this->_where) ; $i++ ) {
      if ( $i === 0 ) {
        // Nothing (simplifies the logic!)
      }
      else if ( $this->_where[$i]['group'] === ')' ) {
        // If a group has been used but no conditions were added inside
        // of, we don't want to end up with `()` in the SQL as that is
        // invalid, so add a 1.
        if ( $this->_where[$i-1]['group'] === '(' ) {
          $condition .= '1=1';
        }
        // else nothing
      }
      else if ( $this->_where[$i-1]['group'] === '(' ) {
        // Nothing
      }
      else {
        $condition .= $this->_where[$i]['operator'].' ';
      }

      if ( $this->_where[$i]['group'] !== null ) {
        $condition .= $this->_where[$i]['group'];
      }
      else {
        $condition .= $this->_where[$i]['query'] .' ';
      }
    }
    return $condition;
  }

  /**
   * {@inheritdoc}
   */
  protected function _select()
  {
    $query_string = 'SELECT '.($this->_distinct ? 'DISTINCT ' : '')
      .$this->_build_field( true )
      .'FROM '.$this->_build_table()
      .$this->_build_join()
      .$this->_build_where()
      .$this->_build_order();
    $query_string = $this->_add_range_to_query($query_string);
    $this->_prepare($query_string);

    return $this->_exec();
  }

  /**
   * {@inheritdoc}
   */
  protected function _count()
  {
    $select = $this->_supportsAsAlias ?
      'SELECT COUNT('.$this->_build_field().') as '.$this->_protect_identifiers('cnt') :
      'SELECT COUNT('.$this->_build_field().') '.$this->_protect_identifiers('cnt');

    $query_string = $select
      .' FROM '.$this->_build_table()
      .$this->_build_join()
      .$this->_build_where();
    $query_string = $this->_add_range_to_query($query_string);

    $this->_prepare($query_string);

    return $this->_exec();
  }

  /**
   * Internal function: add range options to a query.
   *
   * This cannot be set protected because it is used in other parts of the
   * database engine.
   *
   * @status tested
   */
  protected function _add_range_to_query($query) {
    $from = isset($this->_offset) ? $this->_offset : 0;
    $count = $this->_limit;
    if (!$count) {
      return $query;
    }
    if ($from == 0) {
      // Easy case: just use a TOP query if we don't have to skip any rows.
      $query = preg_replace('/^\s*SELECT(\s*DISTINCT)?/Dsi', 'SELECT$1 TOP(' . $count . ')', $query);
    }
    else {
      if ($this->_engine_version() >= 11) {
        if (strripos($query, 'ORDER BY') === FALSE) {
          $query = "SELECT Q.*, 0 as TempSort FROM ({$query}) as Q ORDER BY TempSort OFFSET {$from} ROWS FETCH NEXT {$count} ROWS ONLY";
        }
        else {
          $query = "{$query} OFFSET {$from} ROWS FETCH NEXT {$count} ROWS ONLY";
        }
      }
      else {
        // More complex case: use a TOP query to retrieve $from + $count rows, and
        // filter out the first $from rows using a window function.
        $query = preg_replace('/^\s*SELECT(\s*DISTINCT)?/Dsi', 'SELECT$1 TOP(' . ($from + $count) . ') ', $query);
        $query = '
          SELECT * FROM (
            SELECT sub2.*, ROW_NUMBER() OVER(ORDER BY sub2.__line2) AS __line3 FROM (
              SELECT sub1.*, 1 AS __line2 FROM (' . $query . ') AS sub1
            ) as sub2
          ) AS sub3
          WHERE __line3 BETWEEN ' . ($from + 1) . ' AND ' . ($from + $count);
      }
    }
    return $query;
  }

  protected function _engine_version() {
    $query_string = "SELECT CONVERT (varchar,SERVERPROPERTY('productversion')) AS VERSION";
    $resource = $this->database()->resource();
    $stmt = $resource->prepare( $query_string );
    try {
      $stmt->execute();
      $result = new SqlserverResult( $resource, $stmt );
      $version = $result->fetch(\PDO::FETCH_COLUMN);
      return $version;
    }
    catch (PDOException $e) {
      throw new \Exception("An SQL error occurred: " . $e->getMessage());
      error_log("An SQL error occurred: " . $e->getMessage());
      return 0;
    }
  }

  /**
   * Create a set list
   *  @return string
   *  @internal
   */
  protected function _build_set()
  {
    $a = array();

    for ( $i=0 ; $i<count($this->_field) ; $i++ ) {
      $field = $this->_field[$i];

      if ( isset( $this->_noBind[ $field ] ) ) {
        $a[] = $this->_protect_identifiers( $field, TRUE ) .' = '. $this->_noBind[ $field ];
      }
      else {
        $a[] = $this->_protect_identifiers( $field, TRUE ) .' = :'. $this->_safe_bind( $field );
      }
    }

    return ' '.implode(', ', $a).' ';
  }

  /**
   * Add an individual where condition to the query.
   * @internal
   * @param $where
   * @param null $value
   * @param string $type
   * @param string $op
   * @param bool $bind
   */
  protected function _where ( $where, $value=null, $type='AND ', $op="=", $bind=true )
  {
    if ( $where === null ) {
      return;
    }
    else if ( !is_array($where) ) {
      $where = array( $where => $value );
    }

    foreach ($where as $key => $value) {
      $i = count( $this->_where );

      if ( $value === null ) {
        // Null query
        $this->_where[] = array(
          'operator' => $type,
          'group'    => null,
          'field'    => $this->_protect_identifiers($key, TRUE),
          'query'    => $this->_protect_identifiers($key, TRUE) .( $op === '=' ?
              ' IS NULL' :
              ' IS NOT NULL')
        );
      }
      else if ( $bind ) {
        // Binding condition (i.e. escape data)
        $this->_where[] = array(
          'operator' => $type,
          'group' => NULL,
          'field' => $this->_protect_identifiers($key, TRUE),
          'query' => $this->_protect_identifiers($key, TRUE) . ' ' . $op . ' ' . $this->_safe_bind(':where_' . $i),
        );
        $this->bind(':where_' . $i, $value);
      }
      else {
        // Non-binding condition
        $this->_where[] = array(
          'operator' => $type,
          'group'    => null,
          'field'    => null,
          'query'    => $this->_protect_identifiers($key) .' '. $op .' '. $this->_protect_identifiers($value)
        );
      }
    }
  }
}

